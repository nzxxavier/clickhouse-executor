from dingtalkchatbot.chatbot import DingtalkChatbot
import os

alert_token = os.getenv('alert_token')
alert_owner = os.getenv('alert_owner')


def alarm(message):
    # webhook
    webhook = f"https://oapi.dingtalk.com/robot/send?access_token={alert_token}"
    # bot
    bot = DingtalkChatbot(webhook)
    # send
    bot.send_text(message, is_at_all=False, at_mobiles=[f"{alert_owner}"])
