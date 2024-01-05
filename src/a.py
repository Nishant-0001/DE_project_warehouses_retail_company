column_names = [
    "salesLt",
    "revenueGrowth",
    "customer",
    "productQuality",
    "marketingStrategy",
    "development",
    "inventoryManagement",
    "employeeEngagement",
    "operationalEfficiency",
    "profitMargin"
]

modified_column_names = []

for name in column_names:
    # Add an underscore between each word
    modified_name = '_'.join(name.split())
    modified_column_names.append(modified_name)

# Print the modified column names
print(modified_column_names)

