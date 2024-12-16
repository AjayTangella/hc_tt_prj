# Databricks notebook source
dbutils.secrets.get('tt-hc-kv','tt-adls-access-key-dev')

# COMMAND ----------

# Databricks notebook source
storageAccountName = "allianzlife"
storageAccountAccessKey = dbutils.secrets.get('tt-hc-kv', 'tt-adls-access-key-dev')
mountPoints=["gold","silver","bronze","landing","config"]
for mountPoint in mountPoints:
    if not any(mount.mountPoint == f"/mnt/{mountPoint}" for mount in dbutils.fs.mounts()):
        try:
            dbutils.fs.mount(
            source = "wasbs://{}@{}.blob.core.windows.net".format(mountPoint, storageAccountName),
            mount_point = f"/mnt/{mountPoint}",
            extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
            )
            print(f"{mountPoint} mount succeeded!")
        except Exception as e:
            print("mount exception", e)

dbutils.fs.mounts()
