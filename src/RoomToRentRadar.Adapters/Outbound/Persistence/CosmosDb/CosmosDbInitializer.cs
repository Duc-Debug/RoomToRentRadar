namespace RoomToRentRadar.Adapters.Outbound.Persistence.CosmosDb;

using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RoomToRentRadar.Adapters.Outbound.Persistence.CosmosDb.Configurations;
using RoomToRentRadar.Adapters.Outbound.Persistence.CosmosDb.Documents;

public sealed class CosmosDbInitializer
{
    private readonly CosmosClient _cosmosClient;
    private readonly CosmosDbOptions _settings;
    private readonly ILogger<CosmosDbInitializer> _logger;
    private readonly IHostEnvironment? _environment;

    public CosmosDbInitializer(
        CosmosClient cosmosClient,
        IOptions<CosmosDbOptions> options,
        ILogger<CosmosDbInitializer> logger,
        IHostEnvironment? environment = null)
    {
        _cosmosClient = cosmosClient;
        _settings = options.Value;
        _logger = logger;
        _environment = environment;
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            _logger.LogInformation("Initializing Cosmos DB database '{DatabaseName}'...", _settings.DatabaseName);

            var dbResponse = await _cosmosClient.CreateDatabaseIfNotExistsAsync(_settings.DatabaseName, cancellationToken: cancellationToken);
            var database = dbResponse.Database;

            _logger.LogInformation("Creating container '{CampusesContainerName}' (/id)...", _settings.CampusesContainerName);
            var campusesContainerResponse = await database.CreateContainerIfNotExistsAsync(
                new ContainerProperties(_settings.CampusesContainerName, "/id"),
                cancellationToken: cancellationToken);

            var roomDetailsContainerName = string.IsNullOrWhiteSpace(_settings.RoomDetailsContainerName) ? "RoomDetails" : _settings.RoomDetailsContainerName;
            _logger.LogInformation("Creating container '{RoomDetailsContainerName}' (/id)...", roomDetailsContainerName);
            var roomDetailsContainerResponse = await database.CreateContainerIfNotExistsAsync(
                new ContainerProperties(roomDetailsContainerName, "/id"),
                cancellationToken: cancellationToken);

            var listingsContainerName = string.IsNullOrWhiteSpace(_settings.ContainerName) ? "RoomListings" : _settings.ContainerName;
            _logger.LogInformation("Creating container '{ListingsContainerName}' (/campusId)...", listingsContainerName);
            var roomsContainerResponse = await database.CreateContainerIfNotExistsAsync(
                new ContainerProperties(listingsContainerName, "/campusId"),
                cancellationToken: cancellationToken);

            var effectiveContainer = roomsContainerResponse.Container;
            var effectiveProperties = roomsContainerResponse.Resource;
            var partitionKeyPath = effectiveProperties.PartitionKeyPath;

            if (!string.Equals(partitionKeyPath, "/campusId", StringComparison.Ordinal))
            {
                _logger.LogWarning(
                    "Cosmos container '{ContainerName}' currently uses legacy partition key '{PartitionKeyPath}' instead of required '/campusId'. " +
                    "Cosmos DB does not allow in-place partition key changes. Initiating safe zero-data-loss migration...",
                    listingsContainerName,
                    partitionKeyPath);

                var migrationResult = await SafeMigrateToNewContainerAsync(
                    database,
                    effectiveContainer,
                    listingsContainerName,
                    cancellationToken);

                effectiveContainer = migrationResult.Container;
                effectiveProperties = migrationResult.Resource;
            }

            // Automatic cutover migration from legacy "Rooms" container if it exists
            await MigrateLegacyRoomsContainerIfPresentAsync(database, effectiveContainer, roomDetailsContainerResponse.Container, cancellationToken);

            await EnsureSpatialAndCompositeIndexesAsync(effectiveContainer, effectiveProperties, cancellationToken);

            await SeedInitialDataAsync(campusesContainerResponse.Container, effectiveContainer, cancellationToken);

            _logger.LogInformation("Cosmos DB initialization completed successfully.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize Cosmos DB.");
            throw;
        }
    }

    /// <summary>
    /// Checks for a legacy 'Rooms' container (from earlier versions using /id) and copies documents
    /// to 'RoomListings' (/campusId) and 'RoomDetails' (/id) without deleting 'Rooms'.
    /// </summary>
    private async Task MigrateLegacyRoomsContainerIfPresentAsync(
        Database database,
        Container listingsContainer,
        Container roomDetailsContainer,
        CancellationToken cancellationToken)
    {
        try
        {
            var legacyRoomsContainer = database.GetContainer("Rooms");
            var legacyProperties = await legacyRoomsContainer.ReadContainerAsync(cancellationToken: cancellationToken);
            if (legacyProperties?.Resource is null)
            {
                return;
            }

            // Only migrate if the target containers are currently empty
            var countQuery = new QueryDefinition("SELECT VALUE COUNT(1) FROM c");
            using var countIterator = listingsContainer.GetItemQueryIterator<int>(countQuery);
            if (countIterator.HasMoreResults)
            {
                var countRes = await countIterator.ReadNextAsync(cancellationToken);
                if (countRes.FirstOrDefault() > 0)
                {
                    return; // Already populated
                }
            }

            _logger.LogInformation("Legacy 'Rooms' container detected. Copying documents to 'RoomListings' and 'RoomDetails' (legacy container preserved)...");
            var migratedCount = 0;
            using var feedIterator = legacyRoomsContainer.GetItemQueryIterator<Newtonsoft.Json.Linq.JObject>(new QueryDefinition("SELECT * FROM c"));
            while (feedIterator.HasMoreResults)
            {
                var batch = await feedIterator.ReadNextAsync(cancellationToken);
                foreach (var doc in batch)
                {
                    var id = doc["id"]?.ToString();
                    var campusId = doc["campusId"]?.ToString();
                    if (string.IsNullOrWhiteSpace(campusId))
                    {
                        campusId = "campus-ictu";
                        doc["campusId"] = campusId;
                    }

                    // Copy to RoomListings (/campusId)
                    await listingsContainer.UpsertItemAsync(doc, new PartitionKey(campusId), cancellationToken: cancellationToken);

                    // Copy to RoomDetails (/id)
                    if (!string.IsNullOrWhiteSpace(id))
                    {
                        await roomDetailsContainer.UpsertItemAsync(doc, new PartitionKey(id), cancellationToken: cancellationToken);
                    }

                    migratedCount++;
                }
            }

            _logger.LogInformation("Successfully copied {Count} documents from legacy 'Rooms' container. Both target containers are now populated and 'Rooms' is intact.", migratedCount);
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            // Legacy container does not exist; no action needed.
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to copy from legacy 'Rooms' container. Target containers remain functional.");
        }
    }

    /// <summary>
    /// Safely copies data in streaming batches to a new container partitioned by /campusId.
    /// Crucially, the legacy container is NEVER deleted, preventing any data loss and enabling rollback.
    /// </summary>
    private async Task<ContainerResponse> SafeMigrateToNewContainerAsync(
        Database database,
        Container legacyContainer,
        string legacyContainerName,
        CancellationToken cancellationToken)
    {
        var targetContainerName = $"{legacyContainerName}_v2";
        _logger.LogWarning(
            "Creating target container '{TargetContainer}' with partition key '/campusId' (preserving legacy container '{LegacyContainer}')...",
            targetContainerName,
            legacyContainerName);

        var targetContainerProperties = new ContainerProperties(targetContainerName, "/campusId");
        var targetContainerResponse = await database.CreateContainerIfNotExistsAsync(targetContainerProperties, cancellationToken: cancellationToken);
        var targetContainer = targetContainerResponse.Container;

        var totalMigrated = 0;
        try
        {
            using var feedIterator = legacyContainer.GetItemQueryIterator<Newtonsoft.Json.Linq.JObject>(new QueryDefinition("SELECT * FROM c"));
            while (feedIterator.HasMoreResults)
            {
                var page = await feedIterator.ReadNextAsync(cancellationToken);
                foreach (var doc in page)
                {
                    var campusId = doc["campusId"]?.ToString();
                    if (string.IsNullOrWhiteSpace(campusId))
                    {
                        campusId = "campus-ictu";
                        doc["campusId"] = campusId;
                    }

                    await targetContainer.UpsertItemAsync(doc, new PartitionKey(campusId), cancellationToken: cancellationToken);
                    totalMigrated++;
                }

                _logger.LogInformation("Migrated batch of {Count} items (total so far: {Total}).", page.Count, totalMigrated);
            }

            _logger.LogInformation(
                "Safe migration completed successfully: {Total} documents copied from '{LegacyContainer}' to '{TargetContainer}'. " +
                "The legacy container has been preserved intact for backup and rollback.",
                totalMigrated,
                legacyContainerName,
                targetContainerName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred during batch migration from '{LegacyContainer}' to '{TargetContainer}'. Legacy data remains safe.", legacyContainerName, targetContainerName);
            throw;
        }

        return targetContainerResponse;
    }

    private static async Task EnsureSpatialAndCompositeIndexesAsync(Container container, ContainerProperties properties, CancellationToken cancellationToken)
    {
        var indexingPolicy = properties.IndexingPolicy ?? new IndexingPolicy();
        properties.IndexingPolicy = indexingPolicy;
        var modified = false;

        if (!indexingPolicy.SpatialIndexes.Any(index => index.Path.Equals("/location/*", StringComparison.Ordinal)))
        {
            indexingPolicy.SpatialIndexes.Add(new SpatialPath { Path = "/location/*" });
            modified = true;
        }

        if (!HasCompositeIndex(indexingPolicy, "/monthlyRentAmount", "/id"))
        {
            indexingPolicy.CompositeIndexes.Add(new System.Collections.ObjectModel.Collection<CompositePath>
            {
                new() { Path = "/monthlyRentAmount", Order = CompositePathSortOrder.Ascending },
                new() { Path = "/id", Order = CompositePathSortOrder.Ascending }
            });
            indexingPolicy.CompositeIndexes.Add(new System.Collections.ObjectModel.Collection<CompositePath>
            {
                new() { Path = "/monthlyRentAmount", Order = CompositePathSortOrder.Descending },
                new() { Path = "/id", Order = CompositePathSortOrder.Descending }
            });
            modified = true;
        }

        if (!HasCompositeIndex(indexingPolicy, "/areaSquareMeters", "/id"))
        {
            indexingPolicy.CompositeIndexes.Add(new System.Collections.ObjectModel.Collection<CompositePath>
            {
                new() { Path = "/areaSquareMeters", Order = CompositePathSortOrder.Ascending },
                new() { Path = "/id", Order = CompositePathSortOrder.Ascending }
            });
            indexingPolicy.CompositeIndexes.Add(new System.Collections.ObjectModel.Collection<CompositePath>
            {
                new() { Path = "/areaSquareMeters", Order = CompositePathSortOrder.Descending },
                new() { Path = "/id", Order = CompositePathSortOrder.Descending }
            });
            modified = true;
        }

        if (modified)
        {
            await container.ReplaceContainerAsync(properties, cancellationToken: cancellationToken);
        }
    }

    private static bool HasCompositeIndex(IndexingPolicy policy, string path1, string path2)
    {
        return policy.CompositeIndexes.Any(ci =>
            ci.Count >= 2 &&
            ci[0].Path.Equals(path1, StringComparison.Ordinal) &&
            ci[1].Path.Equals(path2, StringComparison.Ordinal));
    }

    private async Task SeedInitialDataAsync(Container campusesContainer, Container roomsContainer, CancellationToken cancellationToken)
    {
        // 1. Seed ICTU Campus
        const string ictuId = "campus-ictu";
        try
        {
            await campusesContainer.ReadItemAsync<CampusDocument>(ictuId, new PartitionKey(ictuId), cancellationToken: cancellationToken);
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            var ictu = new CampusDocument
            {
                Id = ictuId,
                Name = "Trường Đại học Công nghệ Thông tin và Truyền thông - ĐH Thái Nguyên (ICTU)",
                Code = "ICTU",
                Latitude = 21.5852,
                Longitude = 105.8074,
                Address = "Đường Z115, xã Quyết Thắng, TP. Thái Nguyên, Thái Nguyên"
            };

            await campusesContainer.UpsertItemAsync(ictu, new PartitionKey(ictu.Id), cancellationToken: cancellationToken);
            _logger.LogInformation("Seeded default campus: {CampusName}", ictu.Name);
        }

        // 2. Seed Sample Rooms near ICTU if none exist (restricted to Development or explicit SeedSampleData opt-in)
        var isDevelopment = _environment?.IsDevelopment() ?? false;
        if (!_settings.SeedSampleData && !isDevelopment)
        {
            _logger.LogInformation("Skipping sample rooms seeding in non-development environment or when SeedSampleData is false.");
            return;
        }

        var countQuery = new QueryDefinition("SELECT VALUE COUNT(1) FROM c WHERE c.campusId = @campusId")
            .WithParameter("@campusId", ictuId);

        var iterator = roomsContainer.GetItemQueryIterator<int>(countQuery, requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey(ictuId) });
        var hasRooms = false;
        if (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync(cancellationToken);
            if (response.FirstOrDefault() > 0)
            {
                hasRooms = true;
            }
        }

        if (!hasRooms)
        {
            var sampleRooms = new List<RoomListingDocument>
            {
                new()
                {
                    Id = "room-ictu-001",
                    CampusId = ictuId,
                    Title = "Phòng trọ khép kín cao cấp ngõ 89 Z115 sát cổng ICTU",
                    Description = "Phòng mới xây có điều hòa, nóng lạnh, gác xép, wifi tốc độ cao. Cách cổng chính ICTU 200m đi bộ.",
                    MonthlyRentAmount = 1800000,
                    MonthlyRentCurrency = "VND",
                    AreaSquareMeters = 22.0,
                    Address = "Số 15 ngõ 89 đường Z115, Quyết Thắng, TP. Thái Nguyên",
                    Latitude = 21.5865,
                    Longitude = 105.8080,
                    Location = new Microsoft.Azure.Cosmos.Spatial.Point(105.8080, 21.5865),
                    Status = 0, // Available
                    VerificationStatus = 2, // Verified
                    ThumbnailUrl = "https://images.unsplash.com/photo-1522708323590-d24dbb6b0267",
                    CreatedAtUtc = DateTime.UtcNow.AddDays(-2)
                },
                new()
                {
                    Id = "room-ictu-002",
                    CampusId = ictuId,
                    Title = "Ký túc xá tư nhân full đồ ban công thoáng mát",
                    Description = "Full nội thất giường tầng, bàn học, tủ quần áo riêng. Giờ giấc tự do, an ninh camera 24/7.",
                    MonthlyRentAmount = 1200000,
                    MonthlyRentCurrency = "VND",
                    AreaSquareMeters = 18.5,
                    Address = "Đường Tân Thịnh, đối diện cổng phụ ICTU",
                    Latitude = 21.5840,
                    Longitude = 105.8060,
                    Location = new Microsoft.Azure.Cosmos.Spatial.Point(105.8060, 21.5840),
                    Status = 0, // Available
                    VerificationStatus = 2, // Verified
                    ThumbnailUrl = "https://images.unsplash.com/photo-1502672260266-1c1ef2d93688",
                    CreatedAtUtc = DateTime.UtcNow.AddDays(-5)
                },
                new()
                {
                    Id = "room-ictu-003",
                    CampusId = ictuId,
                    Title = "Căn hộ mini 1 phòng ngủ 1 khách cho nhóm sinh viên",
                    Description = "Căn hộ mini rộng rãi, có bếp riêng, máy giặt chung sân phơi tầng thượng. Thích hợp ở 2-3 bạn.",
                    MonthlyRentAmount = 2800000,
                    MonthlyRentCurrency = "VND",
                    AreaSquareMeters = 35.0,
                    Address = "Tổ 4, P. Thịnh Đán, TP. Thái Nguyên",
                    Latitude = 21.5790,
                    Longitude = 105.8020,
                    Location = new Microsoft.Azure.Cosmos.Spatial.Point(105.8020, 21.5790),
                    Status = 0, // Available
                    VerificationStatus = 2, // Verified
                    ThumbnailUrl = "https://images.unsplash.com/photo-1493809842364-78817add7ffb",
                    CreatedAtUtc = DateTime.UtcNow.AddDays(-1)
                },
                new()
                {
                    Id = "room-ictu-004",
                    CampusId = ictuId,
                    Title = "Phòng trọ giá rẻ cho sinh viên năm nhất",
                    Description = "Phòng trọ bình dân có vệ sinh khép kín, gần chợ sinh viên Quyết Thắng.",
                    MonthlyRentAmount = 900000,
                    MonthlyRentCurrency = "VND",
                    AreaSquareMeters = 15.0,
                    Address = "Khu tập thể Z115, TP. Thái Nguyên",
                    Latitude = 21.5890,
                    Longitude = 105.8110,
                    Location = new Microsoft.Azure.Cosmos.Spatial.Point(105.8110, 21.5890),
                    Status = 0, // Available
                    VerificationStatus = 2, // Verified
                    ThumbnailUrl = "https://images.unsplash.com/photo-1598928506311-c55ded91a20c",
                    CreatedAtUtc = DateTime.UtcNow.AddDays(-7)
                },
                new()
                {
                    Id = "room-ictu-005",
                    CampusId = ictuId,
                    Title = "Phòng trọ đã có bạn thuê (Test Occupied - AC4)",
                    Description = "Phòng trọ đã kín người.",
                    MonthlyRentAmount = 1500000,
                    MonthlyRentCurrency = "VND",
                    AreaSquareMeters = 20.0,
                    Address = "Đường Z115, TP. Thái Nguyên",
                    Latitude = 21.5855,
                    Longitude = 105.8075,
                    Location = new Microsoft.Azure.Cosmos.Spatial.Point(105.8075, 21.5855),
                    Status = 1, // Occupied -> Must NOT be returned
                    VerificationStatus = 2, // Verified
                    ThumbnailUrl = null,
                    CreatedAtUtc = DateTime.UtcNow.AddDays(-10)
                },
                new()
                {
                    Id = "room-ictu-006",
                    CampusId = ictuId,
                    Title = "Phòng trọ chưa kiểm duyệt (Test Unverified - AC4)",
                    Description = "Phòng mới đăng chờ admin duyệt.",
                    MonthlyRentAmount = 1600000,
                    MonthlyRentCurrency = "VND",
                    AreaSquareMeters = 20.0,
                    Address = "Đường Z115, TP. Thái Nguyên",
                    Latitude = 21.5855,
                    Longitude = 105.8075,
                    Location = new Microsoft.Azure.Cosmos.Spatial.Point(105.8075, 21.5855),
                    Status = 0, // Available
                    VerificationStatus = 0, // Unverified -> Must NOT be returned
                    ThumbnailUrl = null,
                    CreatedAtUtc = DateTime.UtcNow.AddDays(-1)
                }
            };

            foreach (var room in sampleRooms)
            {
                await roomsContainer.UpsertItemAsync(room, new PartitionKey(room.CampusId), cancellationToken: cancellationToken);
            }

            _logger.LogInformation("Seeded {Count} sample rooms for campus {CampusId}.", sampleRooms.Count, ictuId);
        }
    }
}
