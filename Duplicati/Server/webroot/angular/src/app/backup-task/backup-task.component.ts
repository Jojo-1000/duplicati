import { SimpleChanges } from '@angular/core';
import { Component, Input } from '@angular/core';
import { Router } from '@angular/router';
import { AddOrUpdateBackupData } from '../backup';
import { BackupService } from '../services/backup.service';

@Component({
  selector: 'app-backup-task',
  templateUrl: './backup-task.component.html',
  styleUrls: ['./backup-task.component.less']
})
export class BackupTaskComponent {
  @Input() backup!: AddOrUpdateBackupData;
  state?: any;
  expanded: boolean = false;

  backupName?: string;
  backupId?: string;
  isScheduled: boolean = false;
  isActive: boolean = false;
  isRunning: boolean = false;
  isPaused: boolean = false;
  description?: string;
  lastBackupFinished?: string;
  lastBackupFinishedTime?: string;
  lastBackupFinishedDuration?: string;
  nextScheduledRun?: string;
  nextScheduledRunDate?: string;
  sourceSizeString?: string;
  targetSizeString?: string;
  backupListCount: number = 0;
  progressCurrentFilename?: string;
  progressPhase?: string;
  progressCurrentFilesize?: number;
  progressCurrentFileoffset?: number;
  progressBarPercentage: number = 0;

  get backupIcon(): string {
    if (!this.isActive && this.isScheduled) {
      return 'backupschedule';
    } else if (this.isScheduled && this.isRunning) {
      return 'backuprunning';
    } else if (this.isScheduled && this.isPaused) {
      return 'backuppaused';
    } else {
      return 'backup';
    }
  }

  constructor(private router: Router, private backupService: BackupService) { }

  ngOnChanges(changes: SimpleChanges): void {
    if ('backup' in changes) {
      this.updateBackup(this.backup);
    }
    if ('state' in changes) {
      this.updateProgress();
    }
  }

  updateBackup(b: AddOrUpdateBackupData) {
    this.backupName = b.Backup.Name;
    this.backupId = b.Backup.ID;
    this.isScheduled = b.Backup.Metadata.has('NextScheduledRun');
    this.isActive = b.Backup.ID === this.state?.activeTask.Item2;
    this.isRunning = this.isActive && this.state?.programState === 'Running';
    this.isPaused = this.isActive && this.state?.programState === 'Paused';
    this.description = b.Backup.Description;
    //this.lastBackupFinished = b.Backup.Metadata.get('LastBackupFinished') | parsetimestamp;
    //this.lastBackupTime = backup?.Backup?.Metadata?.LastBackupFinished | parseDate:forceActualDate
    //this.lastBackupDuration = formatDuration(backup?.Backup?.Metadata?.LastBackupDuration || backup?.Backup?.Metadata?.LastDuration)
    //this.nextScheduledRun = backup?.Backup?.Metadata?.NextScheduledRun | parsetimestamp
    //this.nextScheduledRunDate = backup?.Backup?.Metadata?.NextScheduledRun | parseDate:forceActualDate
    this.sourceSizeString = b.Backup.Metadata.get('SourceSizeString');
    this.targetSizeString = b.Backup.Metadata.get('TargetSizeString');
    this.backupListCount = parseInt(b.Backup.Metadata.get('BackupListCount') || '0');
  }

  updateProgress() {
    this.progressPhase = this.state?.lastPgEvent?.Phase;
    this.progressCurrentFilename = this.state?.lastPgEvent?.ProgressCurrentFilename;
    this.progressCurrentFilesize = this.state?.lastPgEvent?.ProgressCurrentFilesize;
    this.progressCurrentFileoffset = this.state?.lastPgEvent?.ProgressCurrentFileoffset;
    if (this.progressCurrentFilesize !== undefined && this.progressCurrentFileoffset !== undefined) {
      this.progressBarPercentage = (1 - (this.progressCurrentFilesize! - this.progressCurrentFileoffset) / this.progressCurrentFilesize) * 100;
    } else {
      this.progressBarPercentage = 0;
    }
  }

  doRun(): void {
    this.backupService.doRun(this.backupId!);
  }
  doRestore(): void { this.router.navigate(['restore', this.backupId!]); }
  doEdit(): void { this.router.navigate(['edit', this.backupId!]); }
  doExport(): void { this.router.navigate(['export', this.backupId!]); }
  doDelete(): void { this.router.navigate(['delete', this.backupId!]); }
  doLocalDb(): void { this.router.navigate(['localdb', this.backupId!]); }
  doCompact(): void {
    this.backupService.doCompact(this.backupId!);
  }
  doCommandLine(): void { this.router.navigate(['commandline', this.backupId!]); }
  doShowLog(): void { this.router.navigate(['log', this.backupId!]); }
  doCreateBugReport(): void {
    this.backupService.doCreateBugReport(this.backupId!);
  }
  doVerifyRemote(): void {
    this.backupService.doVerifyRemote(this.backupId!);
  }
}
