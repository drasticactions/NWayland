using System;
using System.Threading;

namespace NWayland.Server;

/// <summary>
/// Readiness/wake primitive driven by the server event loop. The Linux
/// implementation is <see cref="WaylandEventPoll"/> (epoll + eventfd); on other
/// platforms — where clients are fd-less <see cref="IWaylandServerTransport"/>s
/// whose readiness arrives via <see cref="WaylandTransportSignal"/> — the loop
/// blocks on <see cref="ManagedEventPoll"/> instead.
/// </summary>
internal interface IWaylandEventPoll : IDisposable
{
    /// <summary>Register a file descriptor with initial interest flags.</summary>
    void AddFd(int fd, uint events);

    /// <summary>Update interest flags for a registered file descriptor.</summary>
    void ModFd(int fd, uint events);

    /// <summary>Remove a file descriptor.</summary>
    void RemoveFd(int fd);

    /// <summary>
    /// Block until an fd is ready or <see cref="Wake"/> is called (timeoutMs=-1),
    /// or poll without blocking (timeoutMs=0). Returns the number of fd results
    /// written; wakeups are consumed and not counted.
    /// </summary>
    int Wait(Span<EpollResult> results, int timeoutMs = -1);

    /// <summary>Wake a blocking <see cref="Wait"/>. Thread-safe.</summary>
    void Wake();
}

/// <summary>
/// Pure-managed poll: no fds can be registered; <see cref="Wait"/> blocks on a
/// semaphore until <see cref="Wake"/>. Used on platforms without epoll, where
/// every client is an fd-less transport and all readiness flows through the
/// server's transport-readiness queue (which wakes this poll).
/// </summary>
internal sealed class ManagedEventPoll : IWaylandEventPoll
{
    private readonly SemaphoreSlim _wake = new(0, 1);
    private int _wakeRequested;
    private volatile bool _disposed;

    public void AddFd(int fd, uint events)
        => throw new PlatformNotSupportedException(
            "fd-based clients require the epoll event poll (Linux). " +
            "Use an IWaylandServerTransport without a PollFd on this platform.");

    public void ModFd(int fd, uint events)
        => throw new PlatformNotSupportedException("fd-based clients require the epoll event poll (Linux).");

    public void RemoveFd(int fd)
    {
        // Nothing can have been registered.
    }

    public int Wait(Span<EpollResult> results, int timeoutMs = -1)
    {
        bool woken;
        try
        {
            woken = timeoutMs < 0 ? WaitInfinite() : _wake.Wait(timeoutMs);
        }
        catch (ObjectDisposedException)
        {
            return 0;
        }

        if (woken)
            Volatile.Write(ref _wakeRequested, 0);
        return 0;

        bool WaitInfinite()
        {
            _wake.Wait();
            return true;
        }
    }

    public void Wake()
    {
        if (Interlocked.Exchange(ref _wakeRequested, 1) == 0)
        {
            if (_disposed)
                return;
            try
            {
                _wake.Release();
            }
            catch (SemaphoreFullException)
            {
                // A wake is already pending — nothing to do.
            }
            catch (ObjectDisposedException)
            {
            }
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
        _wake.Dispose();
    }
}
