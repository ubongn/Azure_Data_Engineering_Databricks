
def count_by_region(records: list[Record]) -> Record:
    counts = {}
    for record in records:
        region = record["region"]
        counts[region] = counts.get(region, 0) + 1
    return counts