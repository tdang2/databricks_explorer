# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW GRANT `user1@tridanginnitico.onmicrosoft.com` ON DATABASE default;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANT `user1@tridanginnitico.onmicrosoft.com` ON TABLE apsupp

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW GRANT `user1@tridanginnitico.onmicrosoft.com` ON VIEW restrictedview1;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW GRANT `user1@tridanginnitico.onmicrosoft.com` ON VIEW restrictedview2;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM restrictedview2 LIMIT 10;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM apsupp LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM restrictedview1 LIMIT 10;

# COMMAND ----------

