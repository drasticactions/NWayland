using System;

namespace NWayland.Server;

/// <summary>
/// Abstraction over a single client's byte + fd-slot transport. The default
/// implementation is <see cref="WaylandServerSocket"/> (an AF_UNIX socket with
/// SCM_RIGHTS fd passing, Linux-only). Synthetic implementations (e.g. a
/// waypipe channel demuxer, or an in-memory test loopback) can run the server
/// on platforms without epoll/AF_UNIX by queueing bytes and fd-slot tokens in
/// managed memory.
/// </summary>
/// <remarks>
/// <para><b>fd slots are opaque to the runtime.</b> The parser moves the
/// <c>int</c> values produced by <see cref="TryReadNonBlocking"/> into request
/// arguments verbatim, and the outgoing buffer hands event fd arguments to
/// <see cref="IWaylandSocketWriter.TryWriteNonBlocking"/> verbatim. Whether a
/// value is a kernel file descriptor or a synthetic token is entirely the
/// transport's (and the embedding application's) concern. All fd-slot values
/// that the runtime needs to release — unconsumed request fds, unsent event
/// fds — are released via <see cref="CloseFd"/> on the transport that owns
/// them, never via a raw <c>close(2)</c>.</para>
/// <para><b>Threading:</b> <see cref="TryReadNonBlocking"/>,
/// <see cref="IWaylandSocketWriter.TryWriteNonBlocking"/>, <see cref="ShutdownRead"/>
/// and <see cref="CloseFd"/> are called from the server's dispatch context only.
/// <see cref="SetSignal"/> is called once from the dispatch context when the
/// client is registered; the transport may invoke the signal from any thread.</para>
/// </remarks>
public interface IWaylandServerTransport : IWaylandSocketWriter, IDisposable
{
    /// <summary>
    /// Non-blocking scatter read into two data buffers and two fd-slot buffers
    /// (two of each because the parser's ring buffers may wrap).
    /// </summary>
    /// <returns>
    /// (BytesRead, FdsRead). BytesRead=-1 means "would block" (EAGAIN);
    /// BytesRead=0 means end-of-stream (client disconnected).
    /// </returns>
    (int BytesRead, int FdsRead) TryReadNonBlocking(
        Memory<byte> buffer1, Memory<byte> buffer2,
        Memory<int> fdBuf1, Memory<int> fdBuf2);

    /// <summary>
    /// Stop accepting inbound data. Subsequent reads return end-of-stream.
    /// Writes must remain possible so a final protocol error can be delivered.
    /// </summary>
    void ShutdownRead();

    /// <summary>
    /// True when the read side is broken (e.g. ancillary-data truncation) and
    /// no further inbound data should be trusted.
    /// </summary>
    bool IsReadBroken { get; }

    /// <summary>
    /// The kernel file descriptor to register with epoll for readiness, or
    /// <c>null</c> when the transport has no pollable fd. Transports returning
    /// <c>null</c> receive a <see cref="WaylandTransportSignal"/> via
    /// <see cref="SetSignal"/> and must signal readiness through it.
    /// </summary>
    int? PollFd { get; }

    /// <summary>
    /// Release an fd-slot value owned by this transport's client: an unconsumed
    /// request fd, an unsent event fd, or one explicitly closed by a listener.
    /// For kernel fds this is <c>close(2)</c>; synthetic transports release the
    /// token. Must be idempotent-safe with respect to transport disposal (it can
    /// be called while the client is being torn down).
    /// </summary>
    void CloseFd(int fd);

    /// <summary>
    /// Called once by the server, from the dispatch context, when the client is
    /// registered with the event loop. Transports with a <see cref="PollFd"/>
    /// may ignore it. Transports without one must call
    /// <see cref="WaylandTransportSignal.NotifyReadable"/> whenever new inbound
    /// data/fd-slots become available (including immediately, if data is already
    /// buffered when the signal is installed) and
    /// <see cref="WaylandTransportSignal.NotifyWritable"/> when write capacity
    /// returns after <see cref="IWaylandSocketWriter.TryWriteNonBlocking"/>
    /// returned -1.
    /// </summary>
    void SetSignal(WaylandTransportSignal signal);
}

/// <summary>
/// Thread-safe readiness signal handed to fd-less transports (see
/// <see cref="IWaylandServerTransport.SetSignal"/>). Notifications enqueue a
/// readiness event for the dispatch loop and wake it if it is blocked.
/// Safe to invoke after the server or client has been disposed (the
/// notification is dropped).
/// </summary>
public sealed class WaylandTransportSignal
{
    private readonly WaylandServer _server;
    private readonly WaylandClient _client;

    internal WaylandTransportSignal(WaylandServer server, WaylandClient client)
    {
        _server = server;
        _client = client;
    }

    /// <summary>New inbound data or fd-slots are available to read.</summary>
    public void NotifyReadable() => _server.SignalTransportReadiness(_client, writable: false);

    /// <summary>The transport can accept writes again after reporting EAGAIN.</summary>
    public void NotifyWritable() => _server.SignalTransportReadiness(_client, writable: true);
}
