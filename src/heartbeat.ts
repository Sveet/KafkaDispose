export class HeartbeatManager {
  [Symbol.dispose](): void {
    this.isBeating = false;
  }
  constructor(
    private heartbeat: () => Promise<void>,
    private rate_ms = 250,
  ) {
    this.beat();
  }
  private isBeating = true;
  private async beat() {
    while (this.isBeating) {
      await this.heartbeat();
      await new Promise((resolve) => setTimeout(resolve, this.rate_ms));
    }
  }
}
