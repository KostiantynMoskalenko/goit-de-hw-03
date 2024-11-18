from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

# Створюємо сесію Spark
spark = SparkSession.builder.appName("MyGoitSparkSandbox").getOrCreate()

#1 Завантажуємо датасети і виводимо статистику та перші 10 рядків з кожного з них
products_df = spark.read.csv("./products.csv", header=True)
products_df.show(10)
purchases_df = spark.read.csv("./purchases.csv", header=True)
purchases_df.show(10)
users_df = spark.read.csv("./users.csv", header=True)
users_df.show(10)

products_df.describe().show()
purchases_df.describe().show()
users_df.describe().show()

#2 Як бачимо, в датасетах є рядки з пропущеними даними. Повидаляємо ці рядки і перевіримо результат
products_df = products_df.dropna()
purchases_df = purchases_df.dropna()
users_df = users_df.dropna()

products_df.describe().show()
purchases_df.describe().show()
users_df.describe().show()

#3 Визначимо загальну суму покупок за кожною категорією продуктів.
purch_prod_df = (purchases_df
                 .join(products_df, on= "product_id", how="inner")
                 .withColumn("total_price", round(col("quantity") * col("price"), 2)))
purch_prod_df.show(10)

categories_sum = (purch_prod_df
                .groupBy("category")
                .sum("total_price"))

categories_sum_rnd = ((categories_sum
                          .withColumn("rounded_sum", round(categories_sum["sum(total_price)"], 2)))
                          .drop("sum(total_price)"))
categories_sum_rnd.show()

#4 Визначимо суму покупок за кожною категорією продуктів для вікової категорії від 18 до 25 включно.
all_df = (purch_prod_df
          .join(users_df, on= "user_id", how="inner"))
all_df.show(10)

age_limit_category_sum = (all_df
                          .filter(((col("age") >= 18) & (col("age") <= 25)))
                          .groupBy("category")
                          .sum("total_price"))
age_limit_category_sum_rnd = ((age_limit_category_sum
                              .withColumn("rounded_sum_18_25", round(age_limit_category_sum["sum(total_price)"], 2)))
                              .drop("sum(total_price)"))
age_limit_category_sum_rnd.show()

#5 Визначимо частку покупок за кожною категорією товарів від сумарних витрат для вікової категорії від 18 до 25 років.
age_limit_sum = age_limit_category_sum_rnd.groupBy().sum().collect()[0][0]
percents_sales = (age_limit_category_sum_rnd
                  .withColumn("percents", round((col("rounded_sum_18_25") / age_limit_sum) * 100, 2)))
percents_sales.show()

#6 Виберемо 3 категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років.
top_3_categories = percents_sales.orderBy(col("percents").desc()).limit(3)
top_3_categories.show()

# Закриваємо сесію Spark
spark.stop()