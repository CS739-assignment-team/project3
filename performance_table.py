from prettytable import PrettyTable


table1 = PrettyTable()

table1.field_names = ["Clients count", "1", "5", "10","20", "50", "100"]


table1.add_row(["PUT Latency(ms)", round(1000.0/9700.08,2), round(1000.0/552.76,2), round(1000.0/242.21,2), 14.05, 25.12, 42.01])
table1.add_row(["GET Latency(ms)", round(1000.0/17455.00,2), round(1000.0/1244.33,2), round(1000.0/548.97,2), 3.91, 7.22, 12.08])

print(table1)

table2 = PrettyTable()

table2.field_names = ["Clients count", "1", "5", "10","20", "50", "100"]

table2.add_row(["PUT Throughput(ops/s)", 9700.08, 552.76, 242.21, round(1000/14.05,2), round(1000/25.12,2), round(1000/42.01,2)])
table2.add_row(["GET Throughput(ops/s)", 17455.00, 1244.33, 548.97, round(1000/3.91,2), round(1000/7.22,2), round(1000/12.08,2)])

print(table2)