namespace RoomToRentRadar.Application.Ports.Inbound;

using RoomToRentRadar.Application.DTOs;

/// <summary>
/// Inbound port for browsing verified rental room listings near a student campus (US01).
/// </summary>
public interface IGetRoomListingsUseCase
{
    Task<PagedResult<RoomListingDto>> ExecuteAsync(
        GetRoomListingsQuery query,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Invalidates cached listing pages for a specific campus when rooms are added, modified, or re-verified.
    /// </summary>
    Task InvalidateCacheAsync(
        string campusId,
        CancellationToken cancellationToken = default);
}
