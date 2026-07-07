/** Minimal browser-safe typed event emitter (subset of Node's EventEmitter). */
export class Emitter<Events extends Record<keyof Events, (...args: any[]) => void>> {
  private readonly listeners = new Map<keyof Events, Set<(...args: any[]) => void>>();

  on<E extends keyof Events>(event: E, fn: Events[E]): this {
    let set = this.listeners.get(event);
    if (!set) this.listeners.set(event, (set = new Set()));
    set.add(fn);
    return this;
  }

  once<E extends keyof Events>(event: E, fn: Events[E]): this {
    const wrapper = ((...args: any[]) => {
      this.off(event, wrapper as Events[E]);
      (fn as (...args: any[]) => void)(...args);
    }) as Events[E];
    return this.on(event, wrapper);
  }

  off<E extends keyof Events>(event: E, fn: Events[E]): this {
    this.listeners.get(event)?.delete(fn);
    return this;
  }

  emit<E extends keyof Events>(event: E, ...args: Parameters<Events[E]>): boolean {
    const set = this.listeners.get(event);
    if (!set || set.size === 0) return false;
    for (const fn of [...set]) fn(...args);
    return true;
  }
}
