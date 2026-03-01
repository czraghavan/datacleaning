import pandas as pd

# Accounts Sheet (Contract-level: 1 row per account)
accounts = pd.DataFrame({
    "Account ID": ["ACC-101", "ACC-102", "ACC-103"],
    "Close Date": ["2023-01-15", "2023-03-22", "2022-11-05"],
    "Vendor": ["Acme Corp", "Globex", "Initech"],
    "Total Value": [50000, 12000, 75000]
})

# Usage Sheet (Line-item level: Multiple usage records per account)
usage = pd.DataFrame({
    "Account_Ref": ["ACC-101", "ACC-101", "ACC-102", "ACC-102", "ACC-103"],
    "Month": ["Jan", "Feb", "Jan", "Feb", "Jan"],
    "Active Users": [10, 12, 5, 5, 100],
    "Feature A Clicks": [500, 600, 100, 120, 5000]
})

with pd.ExcelWriter("mock_churn_data.xlsx") as writer:
    accounts.to_excel(writer, sheet_name="CRM Accounts", index=False)
    usage.to_excel(writer, sheet_name="Product Usage", index=False)

print("Mock data generated: mock_churn_data.xlsx")
