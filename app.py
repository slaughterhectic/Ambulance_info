from flask import Flask, render_template, request, redirect, url_for, flash
from confluent_kafka import Producer, KafkaException
import json
import paho.mqtt.client as mqtt
import os

app = Flask(__name__)
app.secret_key = 'your_secret_key'  # Necessary for flash messages

# Kafka Producer configuration using environment variables for security
conf = {
    'bootstrap.servers': "pkc-12576z.us-west2.gcp.confluent.cloud:9092",
    'security.protocol': "SASL_SSL",
    'sasl.mechanism': "PLAIN",
    'sasl.username': "4ZTYCZCKHXZ2J3CT",
    'sasl.password': "HGk2jjaCF+9xkCBjzxuJcOSqsOpf3rMOLOL6KmeMt6roqEzcka1Dak7YbfUKzxVb"
}
producer = Producer(**conf)

# MQTT Broker configuration
mqtt_broker = "test.mosquitto.org"
mqtt_port = 1883
mqtt_topic = "hospital/results"
mqtt_feedback_topic = "hospital/feedback"

# MQTT client setup
mqtt_client = mqtt.Client()

feedback_messages = []

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker")
        client.subscribe(mqtt_feedback_topic)
    else:
        print("Failed to connect, return code %d\n", rc)

def on_message(client, userdata, msg):
    global feedback_messages
    payload = msg.payload.decode()
    feedback_messages.append(payload)
    print("Received feedback message:", payload)  # Debugging statement

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Connect to MQTT broker
mqtt_client.connect(mqtt_broker, mqtt_port, 60)
mqtt_client.loop_start()

@app.route('/')
def index1():
    global feedback_messages
    return render_template('index1.html', feedback_messages=feedback_messages)

@app.route('/submit', methods=['POST'])
def submit():
    patient_id = request.form['patient_id']
    disease_name = request.form['disease_name']

    test_results = {}
    for key, value in request.form.items():
        if key.startswith('test_result_'):
            test_name = key.split('_')[-1]
            test_results[test_name] = value

    patient_data = {
        'patient_id': patient_id,
        'disease_name': disease_name,
        'test_results': test_results
    }

    try:
        producer.produce('topic1', value=json.dumps(patient_data))
        producer.flush()
        flash('Data sent to Kafka successfully!', 'success')
    except KafkaException as e:
        flash(f'Failed to send data to Kafka: {e}', 'danger')

    result = mqtt_client.publish(mqtt_topic, json.dumps(patient_data))
    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        flash('Data sent to MQTT successfully!', 'success')
    else:
        flash('Failed to send data to MQTT', 'danger')

    return redirect(url_for('index1'))

@app.route('/send_feedback', methods=['POST'])
def send_feedback():
    feedback_message = request.form['feedback_message']
    result = mqtt_client.publish(mqtt_feedback_topic, feedback_message)
    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        flash('Feedback sent successfully!', 'success')
    else:
        flash('Failed to send feedback', 'danger')
    return redirect(url_for('index1'))

if __name__ == '__main__':
    app.run(debug=True, port=5002)
