from prettytable import PrettyTable


table = PrettyTable()

table.field_names = ["Clients count", "1", "5", "10","20", "50", "100"]


table.add_row(["successful_rate_without_inconsistency(%)", 100.0, 100.0, 100.0, 98.3, 97.5, 93.0])

print(table)
