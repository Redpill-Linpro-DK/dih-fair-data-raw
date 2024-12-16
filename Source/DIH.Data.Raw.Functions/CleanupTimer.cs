using DIH.Common;
using DIH.Data.Raw.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace DIH.Data.Raw.Functions
{
    public class CleanupTimer
    {
        private DihLogger<CleanupTimer> Logger { get; set; }
        private AuditCleaner AuditCleaner { get; set; }
        private SoftDeleteCleaner SoftDeleteCleaner { get; set; }

        public CleanupTimer(
            ILogger<CleanupTimer> logger,
            AuditCleaner auditCleaner,
            SoftDeleteCleaner softDeleteCleaner)
        {
            Logger = logger.AsDihLogger();
            AuditCleaner = auditCleaner;
            SoftDeleteCleaner = softDeleteCleaner;
        }

        [FunctionName("dataraw-cleanup-timer")]
        public async Task Run([TimerTrigger("7 9 13 * * *")] TimerInfo myTimer) // running once a day (at 13:09:07 off-peak time)
        {
            try
            {
                await AuditCleaner.Purge();
                await SoftDeleteCleaner.Purge();
            }
            catch (Exception e)
            {
                Logger.DihError(e);
                throw;
            }
        }
    }
}

