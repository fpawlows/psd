from kafka import KafkaConsumer
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import json

# Konfiguracja konsumenta Kafka
consumer = KafkaConsumer(
    'Alarm',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='alarm-visualizer',
    value_deserializer=lambda x: x.decode('utf-8')
)

# Bufory na dane
timestamps = []
temperatures = []

print("Listening to Kafka topic 'Alarm'...")

# Czytanie i parsowanie komunikatów
for message in consumer:
    print("Received:", message.value)
    try:
        # Oczekiwany format: "ALARM! Temp < 0: -12.0 | Sensor: 123 | Time: 2024-05-01T12:34:56Z"
        parts = message.value.split('|')
        temp_str = parts[0].split(':')[-1].strip()
        time_str = parts[2].split(':', 1)[-1].strip()

        temperature = float(temp_str)
        timestamp = datetime.fromisoformat(time_str.replace('Z', '+00:00'))

        temperatures.append(temperature)
        timestamps.append(timestamp)

        # Ogranicz liczbę punktów na wykresie (np. do 100)
        if len(timestamps) > 100:
            timestamps.pop(0)
            temperatures.pop(0)

        # Wykres
        plt.clf()
        plt.plot(timestamps, temperatures, marker='o', linestyle='-', color='red')
        plt.title("Temperatury poniżej zera – ALARMY")
        plt.xlabel("Czas")
        plt.ylabel("Temperatura (°C)")
        plt.gcf().autofmt_xdate()
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))

        plt.pause(0.1)

    except Exception as e:
        print("Błąd parsowania wiadomości:", e)

