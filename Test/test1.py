import sympy as sp

total = 615000
lv = 4.4 / 100 / 12
mns = 30 * 12
i = 1

expr = (total * lv * (1 + lv) ** mns) / ((1 + lv) ** mns - 1)
result = expr.evalf(subs={sp.Symbol('total'): total, sp.Symbol('lv'): lv, sp.Symbol('mns'): mns, sp.Symbol('i'): i})

print(result)


if __name__ == '__main__':
    pass

