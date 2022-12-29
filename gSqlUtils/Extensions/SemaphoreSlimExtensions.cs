using System;
using System.Threading;
using System.Threading.Tasks;

namespace gpower2.gSqlUtils.Extensions
{
    public static class SemaphoreSlimExtensions
    {
        private static readonly TimeSpan _infiniteTimeout = TimeSpan.FromMilliseconds(-1);

        public static Task LockAsync(this SemaphoreSlim semaphoreSlim)
        {
            return semaphoreSlim.LockAsync(_infiniteTimeout, CancellationToken.None);
        }

        public static Task LockAsync(this SemaphoreSlim semaphoreSlim, TimeSpan timeout)
        {
            return semaphoreSlim.LockAsync(timeout, CancellationToken.None);
        }

        public static Task LockAsync(this SemaphoreSlim semaphoreSlim, CancellationToken cancellationToken)
        {
            return semaphoreSlim.LockAsync(_infiniteTimeout, cancellationToken);
        }

        public static Task LockAsync(this SemaphoreSlim semaphoreSlim, TimeSpan timeout, CancellationToken cancellationToken)
        {
            #if NET40
            return Task.Factory.StartNew(() => semaphoreSlim.Wait(timeout, cancellationToken), cancellationToken);
            #else
            return semaphoreSlim.WaitAsync(timeout, cancellationToken);
            #endif
        }
    }
}
