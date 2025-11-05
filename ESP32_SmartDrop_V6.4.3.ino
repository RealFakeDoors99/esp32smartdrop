#include <Arduino.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <Preferences.h>
#include <ArduinoJson.h>
#include <FS.h>
#include <LittleFS.h>
#include <WebServer.h>
#include <DNSServer.h>

// ---------- Version ----------
static const char* FW_VERSION = "V6.4.3";

// ---------- Forward declarations ----------
struct Btn;
bool pollButton(Btn &b);
void publishLidState(bool retain);
void serviceHeartbeat();
void publishHADiscovery();
void publishKeysDiscovery();
void publishKeysStateAll();
void publishFirmwareVersion(bool retain);
String getTopic(const char* baseSuffix);
String getHATopic(const char* component, const char* entitySuffix, const char* action);

// ======================== Pinout =============================
#define PIN_BTN1        16
#define PIN_BTN2        17
#define PIN_BTN3        18
#define PIN_BTN4        19
#define PIN_BTN_OPEN    21
#define PIN_BUZZER      23
#define PIN_LATCH       27
#define PIN_LID         33
#define LID_OPEN_LEVEL  LOW

// ======================== Config Portal ======================
#define AP_PASSWORD           "SmartDrop123"
#define DNS_PORT              53
#define WIFI_CONNECT_TIMEOUT  30000UL      // 30s
#define MQTT_CONNECT_DELAY    4000UL

// ======================== Topics / Names =====================
static const char* BASE_DEVICE_NAME         = "smartdrop";
static const char* TOPIC_SUFFIX_LWT         = "availability";
static const char* TOPIC_SUFFIX_LID         = "lid";
static const char* TOPIC_SUFFIX_EVENT       = "event";
static const char* TOPIC_SUFFIX_PASSWORD    = "password";
static const char* TOPIC_SUFFIX_FW_VERSION  = "fw_version";
static const char* TOPIC_SUFFIX_SET_PW      = "set/password";
static const char* TOPIC_SUFFIX_CMD_OPEN    = "cmd/open";
static const char* TOPIC_SUFFIX_KEYS_NAME    = "keys/name";
static const char* TOPIC_SUFFIX_KEYS_CODE    = "keys/code";
static const char* TOPIC_SUFFIX_KEYS_CREATE  = "keys/create";
static const char* TOPIC_SUFFIX_KEYS_DELETE  = "keys/delete";
static const char* TOPIC_SUFFIX_KEYS_COUNT   = "keys/count";
static const char* TOPIC_SUFFIX_KEYS_SUMMARY = "keys/summary";
#define HA_DISCOVERY_PREFIX "homeassistant"

// ======================== Behavior constants =================
#define BTN_DEBOUNCE_MS   50
#define HEARTBEAT_MS      20000
#define BEEP_SHORT_MS     40
#define BEEP_MEDIUM_MS    200
#define BEEP_LONG_MS      700
#define BEEP_GAP_MS       100
#define PW_TIMEOUT_MS     8000
#define LATCH_PULSE_MS    50
#define MAX_KEYS          128
#define MAX_NAME_LEN      24
#define MAX_CODE_LEN      6

// ======================== Globals ============================
WiFiClient espClient;
PubSubClient mqtt(espClient);
Preferences prefs;

String g_uniqueId; // e.g. "smartdrop123456"
String g_password; // master pass (digits 1..4)
String g_typed = "";
unsigned long g_lastKeyMs             = 0;
unsigned long g_lastMQTTAttemptMs     = 0;
bool latchActive                      = false;
unsigned long latchEndMs              = 0;
int lastLidStable                     = HIGH;
unsigned long lidLastChangeMs         = 0;
unsigned long lastHeartbeatMs         = 0;

// Cached topic strings
String g_deviceJson;
String g_topicSetPw;
String g_topicCmdOpen;
String g_topicKeysName;
String g_topicKeysCode;
String g_topicKeysCreate;
String g_topicKeysDelete;

// ---------- Config stored on LittleFS ----------
struct AppConfig {
  String wifi_ssid;
  String wifi_pass;
  String mqtt_host;
  uint16_t mqtt_port = 1883;
  String mqtt_user;
  String mqtt_pass;
};
AppConfig g_cfg;

WebServer server(80);
DNSServer dns;

bool portalRunning = false;

// ======================== Beeper Queue ======================
struct BeepSeg { bool on; uint16_t dur; };
const int BEEP_Q_CAP = 16;
BeepSeg beepQ[BEEP_Q_CAP];
int qHead = 0, qTail = 0;
bool buzzerOn = false;
unsigned long segEndMs = 0;

bool enqueueSeg(bool on, uint16_t dur) {
  int nextTail = (qTail + 1) % BEEP_Q_CAP;
  if (nextTail == qHead) return false;
  beepQ[qTail] = { on, dur };
  qTail = nextTail;
  return true;
}
void beepSimple(uint16_t dur) { enqueueSeg(true, dur); enqueueSeg(false, 2); }
void beepGap(uint16_t dur)    { enqueueSeg(false, dur); }
void beepShort()              { beepSimple(BEEP_SHORT_MS); }
void beepMedium()             { beepSimple(BEEP_MEDIUM_MS); }
void beepLong()               { beepSimple(BEEP_LONG_MS); }
void beepSuccessPattern() {
  beepSimple(BEEP_SHORT_MS); beepGap(BEEP_GAP_MS);
  beepSimple(BEEP_SHORT_MS); beepGap(BEEP_GAP_MS);
  beepSimple(BEEP_MEDIUM_MS);
}
void serviceBeeper() {
  unsigned long now = millis();
  if (now >= segEndMs) {
    if (qHead != qTail) {
      BeepSeg s = beepQ[qHead];
      qHead = (qHead + 1) % BEEP_Q_CAP;
      buzzerOn = s.on;
      digitalWrite(PIN_BUZZER, buzzerOn ? HIGH : LOW);
      segEndMs = now + s.dur;
    } else {
      if (buzzerOn) {
        digitalWrite(PIN_BUZZER, LOW);
        buzzerOn = false;
      }
      segEndMs = now + 1;
    }
  }
}

// ======================== Button Debounce ===================
struct Btn {
  const char* name;
  uint8_t pin;
  bool lastReading;
  bool stable;
  unsigned long lastChange;
};
Btn btns[] = {
  { "1",   PIN_BTN1,     HIGH, HIGH, 0 },
  { "2",   PIN_BTN2,     HIGH, HIGH, 0 },
  { "3",   PIN_BTN3,     HIGH, HIGH, 0 },
  { "4",   PIN_BTN4,     HIGH, HIGH, 0 },
  { "OPEN",PIN_BTN_OPEN, HIGH, HIGH, 0 }
};
const int BTN_COUNT = sizeof(btns)/sizeof(btns[0]);

bool pollButton(Btn &b) {
  bool reading = digitalRead(b.pin);
  unsigned long now = millis();
  if (reading != b.lastReading) {
    b.lastChange = now;
    b.lastReading = reading;
  }
  if ((now - b.lastChange) > BTN_DEBOUNCE_MS) {
    if (reading != b.stable) {
      b.stable = reading;
      if (b.stable == LOW) {
        Serial.printf("BTN %s: PRESSED\n", b.name);
        return true;
      } else {
        Serial.printf("BTN %s: RELEASED\n", b.name);
      }
    }
  }
  return false;
}

// ======================== Latch Control ====================
void triggerLatch() {
  if (!latchActive) {
    latchActive = true;
    latchEndMs = millis() + LATCH_PULSE_MS;
    digitalWrite(PIN_LATCH, HIGH); // ACTIVE HIGH
    Serial.println("Latch: PULSE START");
  }
}
void serviceLatch() {
  if (latchActive && millis() >= latchEndMs) {
    digitalWrite(PIN_LATCH, LOW);
    latchActive = false;
    Serial.println("Latch: PULSE END");
  }
}

// ======================== Helper Topics ====================
String getTopic(const char* baseSuffix) {
  return String(BASE_DEVICE_NAME) + "/" + g_uniqueId + "/" + String(baseSuffix);
}
String getHATopic(const char* component, const char* entitySuffix, const char* action = "config") {
  return String(HA_DISCOVERY_PREFIX) + "/" + String(component) + "/" + g_uniqueId + "/" + String(entitySuffix) + "/" + String(action);
}

// ======================== Config Storage ===================
const char* CONFIG_PATH = "/config.json";

bool saveConfigToFS(const AppConfig& cfg) {
  DynamicJsonDocument doc(512);
  doc["wifi_ssid"] = cfg.wifi_ssid;
  doc["wifi_pass"] = cfg.wifi_pass;
  doc["mqtt_host"] = cfg.mqtt_host;
  doc["mqtt_port"] = cfg.mqtt_port;
  doc["mqtt_user"] = cfg.mqtt_user;
  doc["mqtt_pass"] = cfg.mqtt_pass;

  File f = LittleFS.open(CONFIG_PATH, FILE_WRITE);
  if (!f) {
    Serial.println("ERROR: LittleFS open for write failed.");
    return false;
  }
  serializeJson(doc, f);
  f.close();
  Serial.println("Saved /config.json");
  return true;
}

bool loadConfigFromFS(AppConfig& cfg) {
  if (!LittleFS.exists(CONFIG_PATH)) return false;
  File f = LittleFS.open(CONFIG_PATH, FILE_READ);
  if (!f) return false;

  DynamicJsonDocument doc(512);
  DeserializationError err = deserializeJson(doc, f);
  f.close();
  if (err) {
    Serial.printf("Config parse error: %s\n", err.c_str());
    return false;
  }

  cfg.wifi_ssid = doc["wifi_ssid"] | "";
  cfg.wifi_pass = doc["wifi_pass"] | "";
  cfg.mqtt_host = doc["mqtt_host"] | "";
  cfg.mqtt_port = (uint16_t)(doc["mqtt_port"] | 1883);
  cfg.mqtt_user = doc["mqtt_user"] | "";
  cfg.mqtt_pass = doc["mqtt_pass"] | "";

  bool ok = cfg.wifi_ssid.length() && cfg.mqtt_host.length();
  Serial.printf("Loaded /config.json (ok=%d)\n", ok);
  return ok;
}

void clearConfigFS() {
  if (LittleFS.exists(CONFIG_PATH)) LittleFS.remove(CONFIG_PATH);
}

// ======================== Config Portal ====================

const char CONFIG_PAGE[] PROGMEM = R"HTML(
<!doctype html><html><head>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>SmartDrop Setup</title>
<style>
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Helvetica,Arial,sans-serif;margin:0;padding:24px;background:#0b1020;color:#e6e8ef}
.card{max-width:640px;margin:0 auto;background:#131a2a;border-radius:16px;box-shadow:0 10px 30px rgba(0,0,0,.3);padding:24px}
h1{margin:0 0 12px;font-size:22px}
label{display:block;margin:14px 0 6px;font-size:14px;color:#aab3c5}
input{width:100%;padding:10px 12px;border:1px solid #2a3550;border-radius:10px;background:#0f1524;color:#e6e8ef}
button{margin-top:16px;width:100%;padding:12px 14px;border:0;border-radius:12px;background:#4e8cff;color:white;font-weight:600;cursor:pointer}
.note{font-size:12px;color:#95a1b8;margin-top:10px}
</style>
</head><body>
<div class="card">
<h1>SmartDrop Setup</h1>
<form method="POST" action="/save">
<label>Wi-Fi SSID</label>
<input name="wifi_ssid" required>
<label>Wi-Fi Password</label>
<input name="wifi_pass" type="password">
<label>MQTT Host/IP</label>
<input name="mqtt_host" required>
<label>MQTT Port</label>
<input name="mqtt_port" type="number" value="1883" min="1" max="65535" required>
<label>MQTT Username</label>
<input name="mqtt_user">
<label>MQTT Password</label>
<input name="mqtt_pass" type="password">
<button type="submit">Save & Connect</button>
<p class="note">After saving, the device will try Wi-Fi + MQTT immediately. If it fails, this setup network stays up.</p>
</form>
</div>
</body></html>
)HTML";

void handleRoot() {
  server.send(200, "text/html; charset=utf-8", CONFIG_PAGE);
}

void handleNotFound() {
  // Captive redirect to /
  server.sendHeader("Location", String("http://") + WiFi.softAPIP().toString() + "/", true);
  server.send(302, "text/plain", "");
}

volatile bool g_requestConnect = false;
AppConfig g_pendingCfg;

void handleSave() {
  AppConfig tmp;
  if (server.hasArg("wifi_ssid")) tmp.wifi_ssid = server.arg("wifi_ssid");
  if (server.hasArg("wifi_pass")) tmp.wifi_pass = server.arg("wifi_pass");
  if (server.hasArg("mqtt_host")) tmp.mqtt_host = server.arg("mqtt_host");
  if (server.hasArg("mqtt_port")) tmp.mqtt_port = (uint16_t) server.arg("mqtt_port").toInt();
  if (server.hasArg("mqtt_user")) tmp.mqtt_user = server.arg("mqtt_user");
  if (server.hasArg("mqtt_pass")) tmp.mqtt_pass = server.arg("mqtt_pass");

  if (tmp.wifi_ssid.length() == 0 || tmp.mqtt_host.length() == 0 || tmp.mqtt_port == 0) {
    server.send(400, "text/plain", "Missing required fields.");
    return;
  }

  if (!saveConfigToFS(tmp)) {
    server.send(500, "text/plain", "Failed saving configuration.");
    return;
  }

  // Respond first
  server.send(200, "text/html", "<html><body><h3>Saved. Attempting to connect...</h3><p>You can close this window.</p></body></html>");

  // Signal main loop to connect (avoid long blocking inside the request handler)
  g_pendingCfg = tmp;
  g_requestConnect = true;
  Serial.println("POST /save processed; will attempt Wi-Fi+MQTT connect.");
}

void startAPPortal() {
  if (portalRunning) return;
  portalRunning = true;

  Serial.println("Starting setup portal...");
  WiFi.mode(WIFI_AP_STA);               // keep AP while we try STA
  WiFi.softAP(g_uniqueId.c_str(), AP_PASSWORD);

  dns.start(DNS_PORT, "*", WiFi.softAPIP());
  server.on("/", HTTP_GET, handleRoot);
  server.on("/save", HTTP_POST, handleSave);
  server.onNotFound(handleNotFound);
  server.begin();

  Serial.printf("AP SSID: %s  PASS: %s  IP: %s\n",
    g_uniqueId.c_str(), AP_PASSWORD, WiFi.softAPIP().toString().c_str());
}

void stopAPPortal() {
  if (!portalRunning) return;
  portalRunning = false;

  server.stop();
  dns.stop();
  WiFi.softAPdisconnect(true);
  Serial.println("Stopped setup portal.");
}

// ======================== WiFi / MQTT ======================================
void onMqttMessage(char* topic, byte* payload, unsigned int len); // fwd

void publishPassword(bool retain=true) {
  mqtt.publish(getTopic(TOPIC_SUFFIX_PASSWORD).c_str(), g_password.c_str(), retain);
  Serial.println("MQTT: published current master password");
}

void publishFirmwareVersion(bool retain = true) {
  String topic = getTopic(TOPIC_SUFFIX_FW_VERSION);
  mqtt.publish(topic.c_str(), FW_VERSION, retain);
  Serial.printf("MQTT: firmware_version -> %s\n", FW_VERSION);
}

bool tryConnectWifiOnce(const AppConfig& cfg) {
  Serial.printf("WiFi: connecting to \"%s\"...\n", cfg.wifi_ssid.c_str());
  WiFi.setSleep(false);
  WiFi.setAutoReconnect(true);
  WiFi.persistent(false);
  WiFi.begin(cfg.wifi_ssid.c_str(), cfg.wifi_pass.c_str());

  unsigned long start = millis();
  wl_status_t st;
  while ((st = WiFi.status()) != WL_CONNECTED && (millis() - start) < WIFI_CONNECT_TIMEOUT) {
    delay(250);
  }
  if (WiFi.status() == WL_CONNECTED) {
    Serial.printf("WiFi connected, IP=%s RSSI=%ld\n", WiFi.localIP().toString().c_str(), WiFi.RSSI());
    return true;
  }
  Serial.printf("WiFi connect failed (status=%d)\n", (int)st);
  return false;
}

bool connectMQTTBlockingOnce() {
  if (WiFi.status() != WL_CONNECTED) return false;

  mqtt.setServer(g_cfg.mqtt_host.c_str(), g_cfg.mqtt_port);
  mqtt.setCallback(onMqttMessage);

  String clientId = g_uniqueId;
  String lwtTopic = getTopic(TOPIC_SUFFIX_LWT);

  Serial.printf("MQTT: connecting to %s:%u as %s...\n",
                g_cfg.mqtt_host.c_str(), g_cfg.mqtt_port, clientId.c_str());

  bool ok = false;
  if (g_cfg.mqtt_user.length()) {
    ok = mqtt.connect(clientId.c_str(),
                      g_cfg.mqtt_user.c_str(), g_cfg.mqtt_pass.c_str(),
                      lwtTopic.c_str(), 1, true, "offline");
  } else {
    ok = mqtt.connect(clientId.c_str(), nullptr, nullptr,
                      lwtTopic.c_str(), 1, true, "offline");
  }

  if (!ok) {
    Serial.printf("MQTT connect failed, rc=%d\n", mqtt.state());
    return false;
  }

  Serial.println("MQTT: connected.");
  mqtt.publish(lwtTopic.c_str(), "online", true);

  // Cache topics
  g_topicSetPw      = getTopic(TOPIC_SUFFIX_SET_PW);
  g_topicCmdOpen    = getTopic(TOPIC_SUFFIX_CMD_OPEN);
  g_topicKeysName   = getTopic(TOPIC_SUFFIX_KEYS_NAME);
  g_topicKeysCode   = getTopic(TOPIC_SUFFIX_KEYS_CODE);
  g_topicKeysCreate = getTopic(TOPIC_SUFFIX_KEYS_CREATE);
  g_topicKeysDelete = getTopic(TOPIC_SUFFIX_KEYS_DELETE);

  // Subs
  mqtt.subscribe(g_topicSetPw.c_str());
  mqtt.subscribe(g_topicCmdOpen.c_str());
  mqtt.subscribe("homeassistant/status");
  mqtt.subscribe(g_topicKeysName.c_str());
  mqtt.subscribe(g_topicKeysCode.c_str());
  mqtt.subscribe(g_topicKeysCreate.c_str());
  mqtt.subscribe(g_topicKeysDelete.c_str());

  // Initial pubs
  publishPassword(true);
  publishLidState(true);
  publishFirmwareVersion(true);
  publishHADiscovery();
  publishKeysDiscovery();
  publishKeysStateAll();

  String bootMsg = String("boot_") + FW_VERSION;
  mqtt.publish(getTopic(TOPIC_SUFFIX_EVENT).c_str(), bootMsg.c_str(), false);
  return true;
}

void connectMQTTIfNeeded() {
  if (WiFi.status() != WL_CONNECTED) return;
  if (mqtt.connected()) return;
  unsigned long now = millis();
  if (now - g_lastMQTTAttemptMs < MQTT_CONNECT_DELAY) return;
  g_lastMQTTAttemptMs = now;
  (void)connectMQTTBlockingOnce();
}

// ======================== Password / UI Logic ==============================
void resetTyped() { g_typed = ""; g_lastKeyMs = 0; }

void triggerUnlockEvent(const char* who) {
  DynamicJsonDocument doc(256);
  doc["unlock"] = who;
  String s; serializeJson(doc, s);
  mqtt.publish(getTopic(TOPIC_SUFFIX_EVENT).c_str(), s.c_str(), false);
}

void handleKey(const char* key); // fwd

// ======================== Heartbeat ========================================
static inline int qDepth() { return (qTail - qHead + BEEP_Q_CAP) % BEEP_Q_CAP; }
void serviceHeartbeat() {
  unsigned long now = millis();
  if (now - lastHeartbeatMs < HEARTBEAT_MS) return;
  lastHeartbeatMs = now;
  Serial.printf("fw=%s id=%s uptime_ms=%lu\n", FW_VERSION, g_uniqueId.c_str(), now);
}

// ======================== HA Discovery ====================================
String deviceJsonBlock() { return g_deviceJson; }

void publishHADiscovery() {
  String lwtTopicStr = getTopic(TOPIC_SUFFIX_LWT);
  StaticJsonDocument<512> doc;
  String cfgPayload;
  JsonArray avail;

  // Lid binary_sensor
  String lidCfgTopic = getHATopic("binary_sensor", "lid");
  doc.clear();
  doc["name"] = "SmartDrop Lid";
  doc["unique_id"] = g_uniqueId + String("_lid");
  doc["state_topic"] = getTopic(TOPIC_SUFFIX_LID);
  doc["payload_on"] = "open";
  doc["payload_off"] = "closed";
  doc["device_class"] = "opening";
  avail = doc.createNestedArray("availability");
  avail.add<JsonObject>()["topic"] = lwtTopicStr;
  doc["payload_available"] = "online";
  doc["payload_not_available"] = "offline";
  doc["device"] = serialized(g_deviceJson);
  cfgPayload = ""; serializeJson(doc, cfgPayload);
  mqtt.publish(lidCfgTopic.c_str(), cfgPayload.c_str(), true);

  // Open Latch button
  String btnCfgTopic = getHATopic("button", "open");
  doc.clear();
  doc["name"] = "Open Latch";
  doc["unique_id"] = g_uniqueId + String("_open");
  doc["command_topic"] = g_topicCmdOpen;
  avail = doc.createNestedArray("availability");
  avail.add<JsonObject>()["topic"] = lwtTopicStr;
  doc["payload_available"] = "online";
  doc["payload_not_available"] = "offline";
  doc["device"] = serialized(g_deviceJson);
  cfgPayload = ""; serializeJson(doc, cfgPayload);
  mqtt.publish(btnCfgTopic.c_str(), cfgPayload.c_str(), true);

  // Firmware Version sensor
  String fwVerCfgTopic = getHATopic("sensor", "fw_version");
  doc.clear();
  doc["name"] = "Firmware Version";
  doc["unique_id"] = g_uniqueId + String("_fw_version");
  doc["state_topic"] = getTopic(TOPIC_SUFFIX_FW_VERSION);
  doc["entity_category"] = "diagnostic";
  doc["icon"] = "mdi:information-outline";
  avail = doc.createNestedArray("availability");
  avail.add<JsonObject>()["topic"] = lwtTopicStr;
  doc["payload_available"] = "online";
  doc["payload_not_available"] = "offline";
  doc["device"] = serialized(g_deviceJson);
  cfgPayload = ""; serializeJson(doc, cfgPayload);
  mqtt.publish(fwVerCfgTopic.c_str(), cfgPayload.c_str(), true);

  Serial.println("MQTT: published HA discovery.");
}

void publishKeysDiscovery() {
  String lwtTopicStr = getTopic(TOPIC_SUFFIX_LWT);
  StaticJsonDocument<512> doc;
  String cfgPayload;
  JsonArray avail;

  // Pair Name (write-only)
  String nameCfgTopic = getHATopic("text", "keys_name");
  doc.clear();
  doc["name"] = "Pair Name";
  doc["unique_id"] = g_uniqueId + String("_keys_name");
  doc["command_topic"] = g_topicKeysName;
  doc["entity_category"] = "config";
  avail = doc.createNestedArray("availability");
  avail.add<JsonObject>()["topic"] = lwtTopicStr;
  doc["payload_available"] = "online";
  doc["payload_not_available"] = "offline";
  doc["device"] = serialized(g_deviceJson);
  cfgPayload = ""; serializeJson(doc, cfgPayload);
  mqtt.publish(nameCfgTopic.c_str(), cfgPayload.c_str(), true);

  // Pair Code (write-only, visible)
  String codeCfgTopic = getHATopic("text", "keys_code");
  doc.clear();
  doc["name"] = "Pair Code";
  doc["unique_id"] = g_uniqueId + String("_keys_code");
  doc["command_topic"] = g_topicKeysCode;
  doc["pattern"] = String("^[1-4]{1,") + String(MAX_CODE_LEN) + "}$";
  doc["min"] = 1;
  doc["max"] = MAX_CODE_LEN;
  doc["entity_category"] = "config";
  avail = doc.createNestedArray("availability");
  avail.add<JsonObject>()["topic"] = lwtTopicStr;
  doc["payload_available"] = "online";
  doc["payload_not_available"] = "offline";
  doc["device"] = serialized(g_deviceJson);
  cfgPayload = ""; serializeJson(doc, cfgPayload);
  mqtt.publish(codeCfgTopic.c_str(), cfgPayload.c_str(), true);

  // Create Pair button
  String createCfgTopic = getHATopic("button", "keys_create");
  doc.clear();
  doc["name"] = "Create Pair";
  doc["unique_id"] = g_uniqueId + String("_keys_create");
  doc["command_topic"] = g_topicKeysCreate;
  avail = doc.createNestedArray("availability");
  avail.add<JsonObject>()["topic"] = lwtTopicStr;
  doc["payload_available"] = "online";
  doc["payload_not_available"] = "offline";
  doc["device"] = serialized(g_deviceJson);
  cfgPayload = ""; serializeJson(doc, cfgPayload);
  mqtt.publish(createCfgTopic.c_str(), cfgPayload.c_str(), true);

  // Delete Pair button
  String deleteCfgTopic = getHATopic("button", "keys_delete");
  doc.clear();
  doc["name"] = "Delete Pair";
  doc["unique_id"] = g_uniqueId + String("_keys_delete");
  doc["command_topic"] = g_topicKeysDelete;
  avail = doc.createNestedArray("availability");
  avail.add<JsonObject>()["topic"] = lwtTopicStr;
  doc["payload_available"] = "online";
  doc["payload_not_available"] = "offline";
  doc["device"] = serialized(g_deviceJson);
  cfgPayload = ""; serializeJson(doc, cfgPayload);
  mqtt.publish(deleteCfgTopic.c_str(), cfgPayload.c_str(), true);

  // Access Codes Count sensor
  String countCfgTopic = getHATopic("sensor", "keys_count");
  doc.clear();
  doc["name"] = "Access Codes Count";
  doc["unique_id"] = g_uniqueId + String("_keys_count");
  doc["state_topic"] = getTopic(TOPIC_SUFFIX_KEYS_COUNT);
  doc["json_attributes_topic"] = getTopic(TOPIC_SUFFIX_KEYS_SUMMARY);
  avail = doc.createNestedArray("availability");
  avail.add<JsonObject>()["topic"] = lwtTopicStr;
  doc["payload_available"] = "online";
  doc["payload_not_available"] = "offline";
  doc["device"] = serialized(g_deviceJson);
  cfgPayload = ""; serializeJson(doc, cfgPayload);
  mqtt.publish(countCfgTopic.c_str(), cfgPayload.c_str(), true);

  Serial.println("MQTT: published keys discovery configs.");
}

// ======================== Named Keys Manager ===============================
DynamicJsonDocument g_keysDoc(16384);
String g_lastNameInput;
String g_lastCodeInput;

bool isValidName(const String &name) {
  if (name.length() < 1 || name.length() > MAX_NAME_LEN) return false;
  for (size_t i=0;i<name.length();++i) {
    char c = name[i];
    bool ok = (c==' ' || c=='_' || c=='-' || isalnum((unsigned char)c));
    if (!ok) return false;
  }
  return true;
}
bool isValidCode(const String &code) {
  if (code.length() < 1 || code.length() > MAX_CODE_LEN) return false;
  for (size_t i=0;i<code.length();++i) {
    char c = (unsigned char)code[i];
    if (c < '1' || c > '4') return false; // Only allow 1-4
  }
  return true;
}
void keysTouchUpdated() { g_keysDoc["updated"] = (uint32_t) (millis()/1000); }
JsonArray keysArray() {
  if (!g_keysDoc.containsKey("keys")) g_keysDoc.createNestedArray("keys");
  return g_keysDoc["keys"].as<JsonArray>();
}
int keysIndexByName(const String &name) {
  JsonArray arr = keysArray();
  for (size_t i=0;i<arr.size();++i) {
    JsonObject o = arr[i].as<JsonObject>();
    if (o["name"].as<String>().equalsIgnoreCase(name)) return (int)i;
  }
  return -1;
}
int keysIndexByCode(const String &code) {
  JsonArray arr = keysArray();
  for (size_t i=0;i<arr.size();++i) {
    JsonObject o = arr[i].as<JsonObject>();
    if ((bool)o["enabled"] && o["code"].as<String>() == code) return (int)i;
  }
  return -1;
}
bool keysSave() {
  String s; serializeJson(g_keysDoc, s);
  prefs.putString("keys_json", s);
  return true;
}
bool keysLoad() {
  String s = prefs.getString("keys_json", "");
  if (s.length() == 0) {
    // Initialize with Master entry
    g_keysDoc.clear();
    JsonArray arr = g_keysDoc.createNestedArray("keys");
    JsonObject m = arr.createNestedObject();
    m["name"] = "Master";
    m["code"] = g_password;
    m["enabled"] = true;
    keysTouchUpdated();
    return keysSave();
  } else {
    DeserializationError err = deserializeJson(g_keysDoc, s);
    if (err) {
      Serial.printf("Keys JSON parse error: %s. Rebuilding.\n", err.c_str());
      g_keysDoc.clear();
      return keysLoad();
    }
    if (!g_keysDoc.containsKey("keys")) g_keysDoc.createNestedArray("keys");
    return true;
  }
}
bool keysAdd(const String &name, const String &code, String &reason) {
  if (!isValidName(name)) { reason = "invalid_name"; return false; }
  if (!isValidCode(code)) { reason = "invalid_code"; return false; }
  JsonArray arr = keysArray();
  if ((int)arr.size() >= MAX_KEYS) { reason = "max_keys"; return false; }
  if (keysIndexByName(name) >= 0) { reason = "duplicate_name"; return false; }
  if (keysIndexByCode(code) >= 0 || code == g_password) { reason = "duplicate_code"; return false; }
  JsonObject o = arr.createNestedObject();
  o["name"] = name; o["code"] = code; o["enabled"] = true;
  keysTouchUpdated(); keysSave(); return true;
}
bool keysDelete(const String &name, String &reason) {
  int idx = keysIndexByName(name);
  if (idx < 0) { reason = "not_found"; return false; }
  JsonArray arr = keysArray(); arr.remove((size_t)idx);
  keysTouchUpdated(); keysSave(); return true;
}
bool keysMatch(const String &code, String &matchedName) {
  int idx = keysIndexByCode(code);
  if (idx >= 0) { matchedName = keysArray()[(size_t)idx]["name"].as<String>(); return true; }
  return false;
}
void publishKeysStateAll() {
  int count = (int)keysArray().size();
  String c = String(count);
  mqtt.publish(getTopic(TOPIC_SUFFIX_KEYS_COUNT).c_str(), c.c_str(), true);
  String s; serializeJson(g_keysDoc, s);
  mqtt.publish(getTopic(TOPIC_SUFFIX_KEYS_SUMMARY).c_str(), s.c_str(), true);
  Serial.printf("Keys: count=%d\n", count);
}

// ======================== Keypad & UI ======================================
void handleKey(const char* key) {
  if (strcmp(key, "OPEN") == 0) {
    if (g_typed.length() == 0) {
      Serial.println("OPEN pressed with no digits.");
      return;
    }
    String who;
    if (keysMatch(g_typed, who)) {
      Serial.printf("PASSWORD OK (named:%s) -> OPENING\n", who.c_str());
      triggerUnlockEvent(who.c_str());
      beepSuccessPattern(); triggerLatch(); resetTyped(); return;
    }
    if (g_typed == g_password) {
      Serial.println("PASSWORD OK (MASTER) -> OPENING");
      triggerUnlockEvent("MASTER");
      beepSuccessPattern(); triggerLatch();
    } else {
      Serial.println("PASSWORD WRONG");
      mqtt.publish(getTopic(TOPIC_SUFFIX_EVENT).c_str(), "local_open_bad_pw", false);
      beepLong();
    }
    resetTyped();
  } else {
    size_t nextLen = g_typed.length() + 1;
    if (nextLen > MAX_CODE_LEN) {
      Serial.println("Password entry over-length -> FAIL");
      mqtt.publish(getTopic(TOPIC_SUFFIX_EVENT).c_str(), "pw_overlength", false);
      beepLong(); resetTyped(); return;
    }
    g_typed += key; g_lastKeyMs = millis();
    Serial.printf("Keyed: %s (len=%d)\n", g_typed.c_str(), g_typed.length());
    beepShort();
  }
}
void servicePasswordTimeout() {
  if (g_typed.length() == 0) return;
  if (millis() - g_lastKeyMs >= PW_TIMEOUT_MS) {
    Serial.println("Password entry timeout.");
    mqtt.publish(getTopic(TOPIC_SUFFIX_EVENT).c_str(), "pw_timeout", false);
    beepMedium(); resetTyped();
  }
}

// ======================== Setup / Loop =====================================
void publishLidState(bool retain) {
  const char* state = (digitalRead(PIN_LID) == LID_OPEN_LEVEL) ? "open" : "closed";
  mqtt.publish(getTopic(TOPIC_SUFFIX_LID).c_str(), state, retain);
  Serial.printf("MQTT: lid -> %s\n", state);
}

void buildDeviceJsonOnce() {
  // Pretty name: "SmartDrop XXXXXX"
  String randomDigits = g_uniqueId.substring(String(BASE_DEVICE_NAME).length());
  String niceName = "SmartDrop " + randomDigits;
  g_deviceJson = String("{\"identifiers\":[\"") + g_uniqueId +
                 "\"],\"name\":\"" + niceName +
                 "\",\"manufacturer\":\"Custom\",\"model\":\"ESP32 SmartDrop\",\"sw_version\":\"" + FW_VERSION + "\"}";
}

void setupUniqueIdAndMasterPass() {
  // Unique ID
  String savedID = prefs.getString("device_id", "");
  if (savedID.length() > 0) {
    g_uniqueId = savedID;
    Serial.printf("Loaded Unique ID from NVS: %s\n", g_uniqueId.c_str());
  } else {
    Serial.println("First boot: Generating new random ID...");
    long randIdNum = (esp_random() % 900000) + 100000; // 6-digit
    g_uniqueId = String(BASE_DEVICE_NAME) + String(randIdNum);
    prefs.putString("device_id", g_uniqueId);
    Serial.printf("New Unique ID '%s' saved to NVS.\n", g_uniqueId.c_str());
  }

  // Master password (digits 1..4)
  String savedPass = prefs.getString("password", "");
  if (savedPass.length() > 0) {
    g_password = savedPass;
    Serial.println("Loaded master password from NVS.");
  } else {
    Serial.println("Generating new random master password (1-4 x6)...");
    long randPwNum = 0;
    for (int i = 0; i < 6; i++) {
      int digit = (esp_random() % 4) + 1; // 1..4
      randPwNum = randPwNum * 10 + digit;
    }
    String newPassword = String(randPwNum);
    prefs.putString("password", newPassword);
    g_password = newPassword;
    Serial.printf("\n*** NEW MASTER PASSWORD: %s ***\n\n", newPassword.c_str());
  }

  buildDeviceJsonOnce();
}

void setup() {
  Serial.begin(115200);
  delay(200);
  Serial.println("\nSmartDrop booting with setup portal (AP+STA)...");

  // Storage
  prefs.begin("smartdrop", false);
  if (!LittleFS.begin(true)) {
    Serial.println("LittleFS mount failed; formatting...");
    LittleFS.format();
    LittleFS.begin(true);
  }

  // Pins
  pinMode(PIN_BTN1, INPUT_PULLUP);
  pinMode(PIN_BTN2, INPUT_PULLUP);
  pinMode(PIN_BTN3, INPUT_PULLUP);
  pinMode(PIN_BTN4, INPUT_PULLUP);
  pinMode(PIN_BTN_OPEN, INPUT_PULLUP);
  pinMode(PIN_LID, INPUT_PULLUP);
  pinMode(PIN_LATCH,  OUTPUT); digitalWrite(PIN_LATCH, LOW);
  pinMode(PIN_BUZZER, OUTPUT); digitalWrite(PIN_BUZZER, LOW);

  // IDs / passwords
  setupUniqueIdAndMasterPass();
  keysLoad(); // uses g_password

  mqtt.setBufferSize(2048);
  mqtt.setKeepAlive(60);

  lastLidStable = digitalRead(PIN_LID);
  lidLastChangeMs = millis();

  // Start portal (AP stays up while we connect)
  startAPPortal();

  // Try connecting immediately if config exists
  if (loadConfigFromFS(g_cfg)) {
    g_requestConnect = true;
    g_pendingCfg = g_cfg;
  }

  Serial.println("Setup complete.");
}

void loop() {
  // Serve portal if running
  if (portalRunning) {
    dns.processNextRequest();
    server.handleClient();
  }

  // When save arrived, attempt Wi-Fi + MQTT (non-blocking trigger here)
  if (g_requestConnect) {
    g_requestConnect = false;
    g_cfg = g_pendingCfg;
    if (tryConnectWifiOnce(g_cfg)) {
      // Wi-Fi OK => stop portal & bring up MQTT
      stopAPPortal();
      (void)connectMQTTBlockingOnce();
    } else {
      // Keep portal up so user can adjust
      Serial.println("Wi-Fi attempt failed; portal remains active.");
    }
  }

  // If Wi-Fi was up but then lost, reopen portal
  static wl_status_t lastSt = WL_NO_SHIELD;
  wl_status_t st = WiFi.status();
  if (lastSt == WL_CONNECTED && st != WL_CONNECTED) {
    Serial.println("Wi-Fi lost — reopening setup portal.");
    startAPPortal();
  }
  lastSt = st;

  // Maintain MQTT
  connectMQTTIfNeeded();
  if (mqtt.connected()) mqtt.loop();

  // Services
  serviceBeeper();
  serviceLatch();
  servicePasswordTimeout();
  serviceHeartbeat();

  // Lid debounce + MQTT publish
  int lidNow = digitalRead(PIN_LID);
  unsigned long now = millis();
  if (lidNow != lastLidStable) {
    if (now - lidLastChangeMs >= BTN_DEBOUNCE_MS) {
      lastLidStable = lidNow;
      const char* s = (lastLidStable == LID_OPEN_LEVEL) ? "open" : "closed";
      Serial.printf("Lid: %s\n", s);
      if (mqtt.connected()) publishLidState(true);
      lidLastChangeMs = now;
    }
  } else {
    lidLastChangeMs = now;
  }

  // Buttons
  for (int i = 0; i < BTN_COUNT; ++i) {
    if (pollButton(btns[i])) {
      handleKey(btns[i].name);
    }
  }

  delay(5);
}

// ======================== MQTT Callback ====================================
void onMqttMessage(char* topic, byte* payload, unsigned int len) {
  String msg;
  msg.reserve(len);
  for (unsigned int i=0; i<len; ++i) msg += (char)payload[i];
  Serial.printf("MQTT RX [%s]: %s\n", topic, msg.c_str());

  if (strcmp(topic, "homeassistant/status") == 0) {
    if (msg == "online") {
      publishHADiscovery();
      publishKeysDiscovery();
      publishLidState(true);
      publishPassword(true);
      publishFirmwareVersion(true);
      publishKeysStateAll();
    }
    return;
  }

  if (strcmp(topic, g_topicSetPw.c_str()) == 0) {
    bool validCodeChars = msg.length() > 0;
    for (size_t i=0; i<msg.length() && validCodeChars; ++i) {
      char c = (unsigned char)msg[i];
      if (c < '1' || c > '4') validCodeChars = false;
    }
    if (validCodeChars && msg.length() <= MAX_CODE_LEN) {
      if (keysIndexByCode(msg) >= 0) {
        mqtt.publish(getTopic(TOPIC_SUFFIX_EVENT).c_str(), "{\"password_update\":\"rejected\",\"reason\":\"duplicate_with_named\"}", false);
      } else {
        g_password = msg;
        prefs.putString("password", g_password);
        publishPassword(true);
        mqtt.publish(getTopic(TOPIC_SUFFIX_EVENT).c_str(), "password_updated", false);
      }
    } else {
      mqtt.publish(getTopic(TOPIC_SUFFIX_EVENT).c_str(), "password_update_rejected", false);
    }
    return;
  }

  if (strcmp(topic, g_topicCmdOpen.c_str()) == 0) {
    mqtt.publish(getTopic(TOPIC_SUFFIX_EVENT).c_str(), "remote_open", false);
    beepSuccessPattern();
    triggerLatch();
    return;
  }

  // ---- Minimal named-keys UI ----
  if (strcmp(topic, g_topicKeysName.c_str()) == 0) { g_lastNameInput = msg; return; }
  if (strcmp(topic, g_topicKeysCode.c_str()) == 0) { g_lastCodeInput = msg; return; }

  if (strcmp(topic, g_topicKeysCreate.c_str()) == 0) {
    String reason;
    if (!isValidName(g_lastNameInput) || !isValidCode(g_lastCodeInput)) {
      mqtt.publish(getTopic(TOPIC_SUFFIX_EVENT).c_str(), "{\"keys\":\"rejected\",\"reason\":\"invalid_input\"}", false);
      beepLong(); return;
    }
    if (keysAdd(g_lastNameInput, g_lastCodeInput, reason)) {
      mqtt.publish(getTopic(TOPIC_SUFFIX_EVENT).c_str(), (String("{\"keys\":\"added\",\"name\":\"") + g_lastNameInput + "\"}").c_str(), false);
      publishKeysStateAll(); beepSuccessPattern();
    } else {
      mqtt.publish(getTopic(TOPIC_SUFFIX_EVENT).c_str(), (String("{\"keys\":\"rejected\",\"reason\":\"") + reason + "\"}").c_str(), false);
      beepLong();
    }
    return;
  }

  if (strcmp(topic, g_topicKeysDelete.c_str()) == 0) {
    String reason;
    if (!isValidName(g_lastNameInput)) {
      mqtt.publish(getTopic(TOPIC_SUFFIX_EVENT).c_str(), "{\"keys\":\"rejected\",\"reason\":\"invalid_name\"}", false);
      beepLong(); return;
    }
    if (g_lastNameInput.equalsIgnoreCase("Master")) {
      mqtt.publish(getTopic(TOPIC_SUFFIX_EVENT).c_str(), "{\"keys\":\"rejected\",\"reason\":\"cannot_delete_master\"}", false);
      beepLong(); return;
    }
    if (keysDelete(g_lastNameInput, reason)) {
      mqtt.publish(getTopic(TOPIC_SUFFIX_EVENT).c_str(), (String("{\"keys\":\"deleted\",\"name\":\"") + g_lastNameInput + "\"}").c_str(), false);
      publishKeysStateAll(); beepMedium();
    } else {
      mqtt.publish(getTopic(TOPIC_SUFFIX_EVENT).c_str(), (String("{\"keys\":\"rejected\",\"reason\":\"") + reason + "\"}").c_str(), false);
      beepLong();
    }
    return;
  }
}
