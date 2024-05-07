# Import necessary libraries
from flask import Flask, render_template, request, redirect, url_for
from confluent_kafka import Producer
import json
import paho.mqtt.client as mqtt

# Initialize Flask app
app = Flask(__name__)

# Kafka Producer configuration
conf = {
    'bootstrap.servers': "pkc-12576z.us-west2.gcp.confluent.cloud:9092",
    'security.protocol': "SASL_SSL",
    'sasl.mechanism': "PLAIN",
    'sasl.username': "ZYREEULMDMG3KM2W",
    'sasl.password': "Bgr9qDoWEu7m279s9WaJlMxmqwX7PZX00AKC4bcYHW29gmTf+ICaF10INqG1GquA"
}
producer = Producer(**conf)

# MQTT Broker configuration
mqtt_broker = "test.mosquitto.org"
mqtt_port = 1883
mqtt_topic = "hospital/results"

# MQTT client setup
mqtt_client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe(mqtt_topic)

def on_publish(client, userdata, mid):
    print("Message published")

# Set MQTT callbacks
mqtt_client.on_connect = on_connect
mqtt_client.on_publish = on_publish

# Connect to MQTT broker
mqtt_client.connect(mqtt_broker, mqtt_port, 60)

# Route for rendering the form
@app.route('/')
def index():
    return render_template('index.html')

# Route for submitting the form
@app.route('/submit', methods=['POST'])
def submit():
    # Get form data
    patient_id = request.form['patient_id']
    disease_name = request.form['disease_name']

    # Extract test results from form data
    test_results = {}
    for key, value in request.form.items():
        if key.startswith('test_result_'):
            test_name = key.split('_')[-1]
            test_results[test_name] = value

    # Construct patient data
    patient_data = {
        'patient_id': patient_id,
        'disease_name': disease_name,
        'test_results': test_results
    }

    # Produce message to Kafka topic
    producer.produce('topic0', value=json.dumps(patient_data))
    producer.flush()

    # Publish message to MQTT topic
    mqtt_client.publish(mqtt_topic, json.dumps(patient_data))

    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True, port=5002)
