import sys
sys.path.insert(0,"/Workspace/Repos/git/Global_Electronics_Retailer/src/lib/")
import ingestors

config_csv = dbutils.widgets.get("config_csv")

ingestors.importfromcsv(config_csv)

config_raw = dbutils.widgets.get("config_raw")