import os
import random
from typing import List, Dict
import hash_util
from minio import Minio
from settings import BUCKET_NAME, BOOKS
from collections import deque


class Bucket(object):
    def __init__(self, size=4):
        self.size = size
        self.bucket = []

    def __contains__(self, item):
        return item in self.bucket

    def __len__(self):
        return len(self.bucket)

    def insert(self, item):
        if not self.is_full():
            self.bucket.append(item)
            return True
        else:
            return False

    def delete(self, item):
        try:
            del self.bucket[self.bucket.index(item)]
            return True
        except ValueError:
            return False

    def is_full(self):
        return len(self.bucket) == self.size

    def __repr__(self):
        return '<Bucket: ' + str(self.bucket) + '>'

    def swap(self, item):
        """被赶出去的时候的交换操作"""
        index = random.choice(range(len(self.bucket)))
        swapped_item = self.bucket[index]
        self.bucket[index] = item
        return swapped_item


class CuckooFilter:
    """可以自动扩容的布谷鸟过滤器"""

    def __init__(self, capacity, bucket_size=4, fingerprint_size=4, max_displacements=500, auto_increase=True):
        # map大小
        self.capacity = capacity
        # hash值大小
        self.fingerprint_size = fingerprint_size
        # 最大驱逐次数
        self.max_displacements = max_displacements
        # 每一个格子的大小
        self.bucket_size = bucket_size
        # map
        self.buckets: List[Bucket] = [Bucket(bucket_size) for _ in range(self.capacity)]
        self.size = 0
        # 为了动态扩容而准备的额外存储空间 记录添加记录 (hash_code,fingerprint)
        self.log = []
        self.auto_increase = auto_increase

    def _expand(self, magnification=1):
        cuckoo = CuckooFilter(
            self.capacity * int(1 + magnification),
            self.bucket_size,
            self.fingerprint_size,
            self.max_displacements,
        )
        for hash_code1, hash_code2, fingerprint, INSERT in self.log:
            position1 = hash_code1 % cuckoo.capacity
            position2 = hash_code2 % cuckoo.capacity
            if INSERT:
                cuckoo.buckets[position1].insert(fingerprint) or cuckoo.buckets[position2].insert(fingerprint)
            else:
                cuckoo.buckets[position1].delete(fingerprint) or cuckoo.buckets[position2].delete(fingerprint)
                pass
        cuckoo.size = self.size
        cuckoo.log = self.log
        self = cuckoo

    @classmethod
    def _get_index(cls, item):
        """计算hash索引"""
        return hash_util.hash_code(item)

    @classmethod
    def _get_alternate_index(cls, index, fingerprint):
        """计算对偶hash索引"""
        return index ^ hash_util.hash_code(fingerprint)

    def insert(self, item) -> bool:
        """存放的是item的指纹"""
        fingerprint = hash_util.fingerprint(item, self.fingerprint_size)
        # hash_code 是为了扩容时使用
        hash_code1 = self._get_index(item)
        position1 = hash_code1 % self.capacity
        hash_code2 = self._get_alternate_index(position1, fingerprint)
        position2 = hash_code2 % self.capacity
        if self.buckets[position1].insert(fingerprint) or self.buckets[position2].insert(fingerprint):
            # 有一个位置成功就OK
            self.log.append((hash_code1, hash_code2, fingerprint, True))
            self.size += 1
            return True
        # 否则准备驱逐....~ 随机选一个赶走
        eviction_index = random.choice([position1, position2])
        for _ in range(self.max_displacements):
            # 当前的换进去 原来的 换出来 再给原来的 找一个新窝
            origin = self.buckets[eviction_index].swap(fingerprint)
            hash_code1 = eviction_index
            hash_code2 = self._get_alternate_index(eviction_index, origin)
            eviction_index = hash_code2 % self.capacity
            if self.buckets[eviction_index].insert(origin):
                self.log.append((hash_code1, hash_code2, origin, True))
                self.size += 1
                return True
        if self.auto_increase:
            self._expand()
            return False
        else:
            raise IndexError('filter is fully')

    def delete(self, item):
        fingerprint = hash_util.fingerprint(item, size=self.fingerprint_size)
        hash_code1 = self._get_index(item)
        position1 = hash_code1 % self.capacity
        hash_code2 = self._get_alternate_index(position1, fingerprint)
        position2 = hash_code2 % self.capacity

        if self.buckets[position1].delete(fingerprint) or self.buckets[position2].delete(fingerprint):
            self.log.append((hash_code1, hash_code2, fingerprint, False))
            self.size -= 1
            return True
        else:
            return False

    def _contain(self, item):
        fingerprint = hash_util.fingerprint(item, self.fingerprint_size)
        position1 = self._get_index(item) % self.capacity
        position2 = self._get_alternate_index(position1, fingerprint) % self.capacity
        return fingerprint in self.buckets[position1] or fingerprint in self.buckets[position2]

    def __contains__(self, item) -> bool:
        """判断是否存在"""
        return self._contain(item)

    def __len__(self):
        return self.size


class MinIOClient:
    # MinIO数据库连接
    def __init__(self, host, port, user, pass_word, secure: bool = False):
        self.client = Minio('{}:{}'.format(host, port), access_key=user, secret_key=pass_word, secure=secure)

    def create_bucket(self, bucket_name: str):
        if self.client.bucket_exists(bucket_name):
            print('bucket:{} already exists'.format(bucket_name))
        else:
            self.client.make_bucket(bucket_name)
            print('successfully create bucket:{}'.format(bucket_name))

    def list_buckets(self):
        """列出数据"""
        buckets = self.client.list_buckets()
        for bucket in buckets:
            print('name:{} create time:{}'.format(bucket.name, bucket.creation_date))

    def remove_bucket(self, bucket_name):
        self.client.remove_bucket(bucket_name)
        print('successfully remove bucket:{}'.format(bucket_name))

    def get_objs(self, bucket_name, **kwargs):
        objs = self.client.list_objects(
            bucket_name=bucket_name,
            prefix=None,
            recursive=True
        )
        return objs

    def get_obj(self, bucket_name, obj_name):
        obj = self.client.get_object(bucket_name=bucket_name, object_name=obj_name)
        return obj

    def download_file(self, bucket_name, obj_name, file_path):
        self.client.fget_object(bucket_name, obj_name, file_path)

    def upload_obj(self, bucket_name, obj_name, file_path):
        return self.client.fput_object(bucket_name, obj_name, file_path)


class MinIO_Connect:
    def __init__(self, client: MinIOClient, bucket_name: str = BUCKET_NAME, books: dict = BOOKS,
                 cuckoo: CuckooFilter = None):
        # minIO连接

        self.client = client
        # minIO 课设调试配置
        self.bucket_name = bucket_name
        self.books = books
        # cuckoo过滤器
        self.cuckoo = cuckoo

    def __contains__(self, obj_name) -> bool:
        return obj_name in self.cuckoo

    def upload_txt(self):
        # 创建 bucket
        self.client.create_bucket(self.bucket_name)
        # 上传的序列表单
        for obj_name, path in self.books.items():
            self.cuckoo.insert(obj_name)
            self.client.upload_obj(self.bucket_name, obj_name, path)

    def download_txt(self):
        for obj_name in self.books.keys():
            self.client.download_file(self.bucket_name, obj_name, '{}.txt'.format(obj_name))


class Index:
    def __init__(self, count, position: tuple):
        self.count = count
        self.position = [position]

    def add(self, position_pair):
        self.count += 1
        self.position.append(position_pair)

    def __repr__(self):
        return 'count:{} position:{}'.format(self.count, self.position)


class Participle:
    """分词索引对象"""

    def __init__(self, dir_path):
        """根据给定目录 读取文件 构建 关键词 - (文件名:index , ....)"""
        self.dir_path = dir_path
        self.files = {}
        # 生成文件 - 文件路径 pair
        self._get_pairs()
        # index['古典小说'] = ((file_name,count,position))
        self.index: Dict[str:Dict[str:Index]] = {}
        # self.index[key_word] : count [(),(),()]
        # 建立索引
        self._create_index()

    def _create_index(self):
        for file_name, file_path in self.files.items():
            with open(file_path, 'r', encoding='utf-8') as file:
                line_index = 1
                while True:
                    # 一行一行的读取
                    line = file.readline()
                    if not line:
                        break
                    else:
                        # NoToDo re 文本提前预处理 消去符号 (有这个必要么？ 不是数据预处理课)
                        # 用空格分词
                        words = line.split(' ')
                        for col_index, word in enumerate(words, 0):
                            if word in self.index:  # 词在
                                if file_name in self.index[word]:  # 文件在
                                    self.index[word][file_name].add((line_index, col_index+1))
                                else:
                                    self.index[word][file_name] = Index(1, (line_index, col_index+1))
                            else:  # 词不在
                                self.index[word] = {file_name: Index(1, (line_index, col_index+1))}
                        # 行索引 + 1
                        line_index += 1

    def _get_pairs(self):
        for file_name in os.listdir(self.dir_path):
            self.files[file_name] = '{}\\{}'.format(self.dir_path, file_name)

    def find(self, key_word):
        """根据关键词查询"""
        return self.index[key_word]


class AdaptiveRadixNode:
    def __init__(self):
        # 路过单词数
        self.pass_by = 0
        # 实际存储数
        self.end = 0
        # 动态体现在这里 只有当有值时 才 扩容
        self.map: Dict[str:AdaptiveRadixNode] = {}

    def keys(self):
        return self.map.keys()

    def values(self):
        return self.map.values()

    def items(self):
        return self.map.items()

    def __repr__(self):
        return ''


class AdaptiveRadixTree:
    def __init__(self):
        self.root = AdaptiveRadixNode()

    def insert(self, word: str) -> bool:
        if word is None:
            return False
        node = self.root

        for char in word:
            if char not in node.map:
                node.map[char] = AdaptiveRadixNode()
            node: AdaptiveRadixNode = node.map[char]
            node.pass_by += 1

        node.end += 1
        return True

    def search(self, word: str):
        if not isinstance(word, str):
            raise KeyError(f'{word} must be type str')

        node = self.root
        for char in word:
            if char not in node.map:
                return 0
            node = node.map[char]
        return node.end

    def delete(self, word):
        try:
            node = self.root
            for char in word:
                node.map[char].pass_by -= 1
                if node.map[char].pass_by == 0:
                    del node.map[char]
                    return
                node = node.map[char]
            node.end -= 1
        except Exception:
            raise KeyError(f'{word} can\'t be found')

    def layer_traversal(self):
        """
        N叉树的层序遍历
        .首先遍历当前层的所有节点
        .根节点入队
        .每一轮记录当前队列的包含的节点数目 表示上一层的节点数目
        .一轮结束后 取出对应数量节点 余下的就是下一层的节点
        """
        ans = []
        queue = deque()
        queue.append(self.root)
        while queue:
            count = len(queue)
            layer = []
            for node_index in range(count):
                cur: AdaptiveRadixNode = queue.popleft()
                # 这里转一下 在控制台 更好观察 layer.append(cur.map.keys())
                layer.append(tuple(cur.map.keys()))

                for node in cur.values():
                    queue.append(node)
            ans.append(layer)
        return ans

    def __repr__(self):
        return str(self.root.map)


def task_1_2_test():
    """第一题/第二题 demo"""
    cuckoo = CuckooFilter(
        capacity=1000,
        bucket_size=4,
        fingerprint_size=4,
        max_displacements=500,
        auto_increase=True
    )
    minio_client = MinIOClient('127.0.0.1', 9000, 'minioadmin', 'minioadmin')
    design = MinIO_Connect(client=minio_client, cuckoo=cuckoo)
    design.upload_txt()
    design.download_txt()


def task_3_test():
    """建立倒排索引"""
    dir_path = r'E:\Learning\GitProject\BigData\BigDataClassDesign\data\index'
    participle = Participle(dir_path)
    # print(participle.find('古典小说'))
    # print(participle.find('历史'))
    # print(participle.find('good'))
    # print(participle.find('see'))
    # print(participle.find('你好'))
    data = participle.find('古典小说')
    # print(list(data.keys()))
    # row_index col_index
    pass


def task_4_test():
    tree = AdaptiveRadixTree()
    keys = ['hello', 'delete', 'mother', 'father', 'small', 'tree', 'node', 'bed', 'be']
    for key in keys:
        tree.insert(key)
    for key in keys:
        print(tree.search(key))
    for layer in tree.layer_traversal():
        print(layer)
    tree.delete('hello')
    print(tree.search('hello'))
    print('not found')

    for layer in tree.layer_traversal():
        print(layer)
    for key in keys:
        tree.delete(key)
        print(tree.search(key))
    tree.insert('hello')
    tree.insert('hi')
    tree.delete('hello')
    for layer in tree.layer_traversal():
        print(layer)


if __name__ == '__main__':
    task_1_2_test()
    task_3_test()
    task_4_test()
