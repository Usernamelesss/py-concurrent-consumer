class MessageHandler:
    """
    The class that actually contains the logic to handle a Kafka message
    """

    def handle(self, message):
        # Handle the message here
        print(message)

    def decode(self, message: bytearray):
        # Optionally decode the binary record to an application class/message/dto
        return f"Message[body={message}]"
