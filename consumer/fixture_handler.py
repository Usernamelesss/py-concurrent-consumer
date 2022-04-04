

class BaseEventHandler:

    def handle(self, message):
        print(message)

    def decode(self, message):
        return f'Message[body={message}]'
