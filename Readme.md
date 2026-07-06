CheckPoint SASE - Task


IoT Temperature Anomaly Detection

Write a Flink job that flags anomalous temperature readings from IoT devices.

• ~2,000 devices, ~100 measurements per device per minute.
• An anomaly = a reading 3 standard deviations from the 1-minute average.
• Print anomalies to the console, e.g.:
    Device 455weg75uew, measurement 95.4 C, time 45587456

Use Java and Flink. Everything else is your call - build tool, layout, and how you generate the input (a small generator is fine).
Use AI if that's how you normally work; We'll just ask you to explain your choices.