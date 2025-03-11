import sys
import os

# 将项目根目录加入 sys.path，假设 tests/ 在项目根目录下
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
