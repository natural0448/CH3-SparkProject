from functools import reduce


rows = [
    ("A", 100), ("A", 200), ("A", 300),
    ("B", 100), ("B", 200), ("B", 300),
    ("A", 500), ("A", 500),
    ("B", 400), ("B", 500), ("B", 600), ("B", 700),
]

print(len(rows))
print(rows[3])

totals = {}
for product_id, amount in rows:
    old_count, old_revenue = totals.get(product_id, (0, 0))
    totals[product_id] = (old_count + 1, old_revenue + amount)

print(totals)


def to_pair(order):
    product_id, amount = order
    return product_id, (1, amount)

def add_pair(totals, pair):
    product_id, (count, revenue) = pair
    old_count, old_revenue = totals.get(product_id, (0, 0))
    totals[product_id] = (old_count + count, old_revenue + revenue)
    return totals

print(to_pair(rows[0]))
print(add_pair({}, ("A", (1, 100))))

print("map연산으로 0번째 - 5번째 자료 정제", list(map(to_pair, rows[:6])))
print("map연산으로 6번째 - 마지막 자료 정제", list(map(to_pair, rows[6:])))
partial_p0 = reduce(add_pair, map(to_pair, rows[:6]), {})
partial_p1 = reduce(add_pair, map(to_pair, rows[6:]), {})
print("map연산으로 정체된 자료를 집계한 reduce결과물 0",partial_p0)
print("map연산으로 정체된 자료를 집계한 reduce결과물 1", partial_p1)


partial_rows = list(partial_p0.items()) + list(partial_p1.items())
final_totals = reduce(add_pair, partial_rows, {})

print(partial_rows)
print(final_totals)

rows[6] = ("A", 400)

p1 = reduce(add_pair, map(to_pair, rows[6:]), {})
final_totals = reduce(add_pair,list(partial_p0.items()) + list(p1.items()),{},)

print(final_totals["A"])  # (5, 1500)
print(final_totals["A"][1] / final_totals["A"][0])  # 300.0