from pymongo import MongoClient
import pandas as pd


class analyse():
	def __init__(self):
		self.host = 'localhost'
		self.port = 27017
		self.db = 'yqw'
		self.tb = 'python'

	# 连接数据库
	def connect(self):
		self.client = MongoClient(host=self.host, port=self.port)
		self.collection = self.client[self.db][self.tb]
		self.data = self.collection.find()
		# print(self.data)
		self.data = list(self.data)
		# print(self.data)
		return self.data

	# 统计某列各值出现次数
	def db_find(self, locname):
		df = pd.DataFrame(self.connect())
		# df2 = df.location.value_counts()  # 可以通过df.colname 来指定某个列，value_counts()在这里进行计数
		try:
			if locname == 'rank_a':  # 排名升降
				df2 = df.rank_a.value_counts()
				# print('rank_a中每个值出现的次数:\n', df2)
			elif locname == 'seller':  # 卖方
				df2 = df.seller.value_counts()
				# print('seller中每个值出现的次数:\n', df2)
			elif locname == 'month_reviews':  # 月评论数
				df2 = df.month_reviews.value_counts()
				# print('month_reviews中每个值出现的次数:\n', df2)
			elif locname == 'lifetime_reviews':  # 终身评论数
				df2 = df.lifetime_reviews.value_counts()
				# print('lifetime_reviews中每个值出现的次数:\n', df2)
			elif locname == 'location':  # 地点
				df2 = df.location.value_counts()
				# print('location中每个值出现的次数:\n', df2)
			else:
				print('请输入正确的列名')
			return df2
		except UnboundLocalError as u:
			pass


	# 找lifetime_reviews最大值所对应的seller
	def db_max(self):
		df = pd.DataFrame(self.connect())
		a = df['lifetime_reviews'].astype(int).idxmax()  # 找到最大值的行索引
		a1 = df['lifetime_reviews'].astype(int).max()  # 找到最大值
		print('评论最多的卖方：', df.loc[a, 'seller'], '评论数为：', a1)
		return a1

	# 找lifetime_reviews最小值所对应的seller
	def db_min(self):
		df = pd.DataFrame(self.connect())
		b = df['lifetime_reviews'].astype(int).idxmax()
		b1 = df['lifetime_reviews'].astype(int).min()
		print('评论最少的卖方：', df.loc[b, 'seller'], '评论数为：', b1)
		return b1


if __name__ == '__main__':
	b = analyse()
	b.connect()
	b.db_find('错误列名')
	print(b.db_find('seller'))
	# b.db_max()
	# b.db_min()
