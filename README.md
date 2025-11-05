An ESP32-based smart drop-box controller with keypad input (1–4), solenoid latch control, lid sensor, buzzer feedback, and native MQTT/Home Assistant integration. On first boot—or whenever Wi-Fi/MQTT is unset or fails—the device starts a captive Wi-Fi setup portal so you can enter credentials without editing code.

Highlights

On-device setup portal: AP SSID = device ID (e.g., smartdrop123456), password SmartDrop123. Open the portal (usually at 192.168.4.1) to enter Wi-Fi + MQTT.

Persistent config: Credentials saved to LittleFS (/config.json). Portal auto-reopens if connection fails.

Home Assistant discovery: Publishes entities for lid state, manual “Open Latch” button, firmware version, and access-code management.

Named access codes: Create/delete 1–4 keypad codes (stored in NVS), with a generated 6-digit master code on first boot.

Robust I/O: Debounced keypad (1–4 + OPEN), lid sensor monitoring, timed latch pulse, and queued beeper patterns.

MQTT events & state: Lid state, boot/status events, key management summary, and more.

Hardware (default pins)

Buttons: 1=16, 2=17, 3=18, 4=19, OPEN=21

Buzzer: 23 · Latch: 27 · Lid switch: 33 (active LOW)

Build

ESP32 Arduino core, libraries: WiFi, PubSubClient, Preferences, ArduinoJson, LittleFS, WebServer, DNSServer.

Enable LittleFS in your partition scheme. Flash the single .ino.

First-Time Setup

Power on; join the AP shown as the device ID (password SmartDrop123).

Visit the portal (typically 192.168.4.1), enter Wi-Fi + MQTT details, Save & Connect.

On success the AP stops; the device connects to Wi-Fi, then MQTT, and announces itself to Home Assistant.

Notes

This project is intended for hobby/tinkering use. Credentials are stored in plaintext on the device and some values are published to MQTT for convenience.

Serial logs are helpful for onboarding (115200 baud).
