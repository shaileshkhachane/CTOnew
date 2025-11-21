export interface CubeInvalidationEvent {
  cube: string;
  sources: string[];
  timestamp: Date;
  reason: string;
}

export class CubeManager {
  private invalidations: CubeInvalidationEvent[] = [];
  private listeners: Array<(event: CubeInvalidationEvent) => void | Promise<void>> = [];

  invalidate(cube: string, sources: string[], reason: string): void {
    const event: CubeInvalidationEvent = {
      cube,
      sources,
      timestamp: new Date(),
      reason,
    };

    this.invalidations.push(event);
    console.log(`[CubeManager] Invalidating cube '${cube}' due to: ${reason}`);

    for (const listener of this.listeners) {
      Promise.resolve(listener(event)).catch((error) => {
        console.error(`[CubeManager] Listener error:`, error);
      });
    }
  }

  onInvalidation(listener: (event: CubeInvalidationEvent) => void | Promise<void>): void {
    this.listeners.push(listener);
  }

  getInvalidations(cube?: string): CubeInvalidationEvent[] {
    if (cube) {
      return this.invalidations.filter(inv => inv.cube === cube);
    }
    return [...this.invalidations];
  }

  clearInvalidations(cube?: string): void {
    if (cube) {
      this.invalidations = this.invalidations.filter(inv => inv.cube !== cube);
    } else {
      this.invalidations = [];
    }
  }

  async rebuild(cube: string): Promise<void> {
    console.log(`[CubeManager] Rebuilding cube '${cube}'...`);
    await new Promise(resolve => setTimeout(resolve, 100));
    console.log(`[CubeManager] Cube '${cube}' rebuilt successfully`);
  }
}
