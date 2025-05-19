from openai import OpenAI
import requests
import sys

sys.stdout.reconfigure(encoding='utf-8')

# 设置正确的 API 密钥和 base_url
client = OpenAI(api_key="", base_url="https://api.deepseek.com")

def get_sentiment_score(text):
    prompt = f"""请对以下微博内容进行情绪倾向打分，范围是1~10，其中：
1~4分表示负面情绪，5分表示中性情绪，6~10分表示正面情绪。请一定只返回一个整数，千万不要解释。如果判断不了的，一律返回5。

微博内容：{text}
"""

    try:
        # 请求 DeepSeek API
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": "你是一个微博情感分析专家"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.5,
            stream=False
        )
        
        if not response or not response.choices:
            print("分析结果为空，返回默认值 5")
            return 5
        # 从返回结果中提取评分
        score_str = response.choices[0].message.content.strip()
        try:
            return float(score_str)
        except ValueError:
            print(f"分析结果：{score_str} 无法转换为数字，返回默认值 5")
            return 5
    except Exception as e:
        print(f"分析失败：{e}")
        return None
