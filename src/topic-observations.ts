type Observed = { data: Set<string | null>; table: Set<string | null> };

/** Only an authoritative registry removal retires a previously observed topic. */
export class TopicObservations {
  private readonly observed = new Map<string, Observed>();

  record(topic: string, kind: "data" | "table", group: string | null | undefined): boolean {
    let entry = this.observed.get(topic);
    if (!entry) {
      entry = { data: new Set(), table: new Set() };
      this.observed.set(topic, entry);
    }
    const groups = entry[kind];
    const value = group ?? null;
    if (groups.has(value)) return false;
    groups.add(value);
    return true;
  }

  groups(topic: string, suffix: "_data" | "_table"): Array<string | null> {
    return [...(this.observed.get(topic)?.[suffix === "_data" ? "data" : "table"] ?? [])];
  }

  reconcile(activeTopics: Iterable<string>): void {
    const active = new Set(activeTopics);
    for (const topic of this.observed.keys()) {
      if (!active.has(topic)) {
        this.observed.delete(topic);
      }
    }
  }

}
