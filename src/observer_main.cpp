// observer/main.cpp
#include <Arduino.h>
#include <SPI.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <Preferences.h>
#include <FS.h>
#include <SPIFFS.h>
#include <Wire.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>
#include <RadioLib.h>
#include <PubSubClient.h>
#include <mbedtls/sha256.h>

// ================= PIN MAP (Heltec WiFi LoRa 32 V3 / V3.2) =================
#define LORA_CS    8
#define LORA_SCK   9
#define LORA_MOSI  10
#define LORA_MISO  11
#define LORA_RST   12
#define LORA_BUSY  13
#define LORA_DIO1  14
#define VEXT_EN    36

// ================= RF PARAMETERS =================
#define FREQ_MHZ   869.618
#define BW_KHZ     62.5
#define SF         8
#define CR_DENOM   8        // 4/8

// ================= FIRMWARE VERSION =================
#define OBSERVER_FW_VER "1.1.6"

// ================= OLED (Heltec V3) =================
#define OLED_SDA   17
#define OLED_SCL   18
Adafruit_SSD1306 display(128, 64, &Wire, -1);
bool displayReady = false;
bool displayDirty = true;
unsigned long lastDisplayMs = 0;
uint8_t oledAddr = 0x3C;
bool vextActiveLow = true;

// PubSubClient default buffer is too small for full hex payloads.
#define MQTT_BUFFER_SIZE 2048

// ================= CONFIG DEFAULTS =================
#ifndef OBSERVER_WIFI_SSID
#define OBSERVER_WIFI_SSID ""
#endif
#ifndef OBSERVER_WIFI_PASS
#define OBSERVER_WIFI_PASS ""
#endif
#ifndef OBSERVER_MQTT_HOST
#define OBSERVER_MQTT_HOST "meshrank.net"
#endif
#ifndef OBSERVER_MQTT_PORT
#define OBSERVER_MQTT_PORT 8883
#endif
#ifndef OBSERVER_MQTT_USER
#define OBSERVER_MQTT_USER ""
#endif
#ifndef OBSERVER_MQTT_PASS
#define OBSERVER_MQTT_PASS ""
#endif
#ifndef OBSERVER_DEVICE_ID
#define OBSERVER_DEVICE_ID ""
#endif
#ifndef OBSERVER_LAT
#define OBSERVER_LAT 0.0
#endif
#ifndef OBSERVER_LON
#define OBSERVER_LON 0.0
#endif
#ifndef OBSERVER_SERIAL_CONFIG
#define OBSERVER_SERIAL_CONFIG 1
#endif

// ================= STORAGE =================
static const char *PREFS_NS = "observer";
static const char *SPOOL_PATH = "/spool.ndjson";
static const size_t MAX_SPOOL_BYTES = 256 * 1024;

// ================= RADIO =================
SX1262 radio = new Module(LORA_CS, LORA_DIO1, LORA_RST, LORA_BUSY);
volatile bool rxFlag = false;

void IRAM_ATTR onDio1() {
  rxFlag = true;
}

static inline bool takeRxFlag() {
  bool got;
  noInterrupts();
  got = rxFlag;
  rxFlag = false;
  interrupts();
  return got;
}

// ================= MQTT =================
WiFiClientSecure tlsClient;
PubSubClient mqttClient(tlsClient);

Preferences prefs;
String wifiSsid;
String wifiPass;
String mqttHost;
int mqttPort = OBSERVER_MQTT_PORT;
String mqttUser;
String mqttPass;
String observerId;
String observerName;
float observerLat = OBSERVER_LAT;
float observerLon = OBSERVER_LON;
bool wifiWasConnected = false;
bool mqttWasConnected = false;

// ================= UTILITIES =================
static inline void toHex(const uint8_t *data, size_t len, char *out) {
  const char *hex = "0123456789ABCDEF";
  for (size_t i = 0; i < len; i++) {
    out[i * 2] = hex[(data[i] >> 4) & 0x0F];
    out[i * 2 + 1] = hex[data[i] & 0x0F];
  }
  out[len * 2] = '\0';
}

static inline String macId() {
  uint64_t mac = ESP.getEfuseMac();
  char buf[13];
  snprintf(buf, sizeof(buf), "%06lX%06lX", (uint32_t)(mac >> 24), (uint32_t)(mac & 0xFFFFFF));
  return String(buf);
}

static inline String sha256Hex(const uint8_t *data, size_t len) {
  uint8_t out[32];
  mbedtls_sha256_context ctx;
  mbedtls_sha256_init(&ctx);
  mbedtls_sha256_starts_ret(&ctx, 0);
  mbedtls_sha256_update_ret(&ctx, data, len);
  mbedtls_sha256_finish_ret(&ctx, out);
  mbedtls_sha256_free(&ctx);
  char buf[65];
  toHex(out, 32, buf);
  return String(buf);
}

static inline void loadConfig() {
  prefs.begin(PREFS_NS, true);
  wifiSsid = prefs.getString("ssid", OBSERVER_WIFI_SSID);
  wifiPass = prefs.getString("pass", OBSERVER_WIFI_PASS);
  observerId = prefs.getString("id", OBSERVER_DEVICE_ID);
  observerName = prefs.getString("name", "");
  observerLat = prefs.getFloat("lat", OBSERVER_LAT);
  observerLon = prefs.getFloat("lon", OBSERVER_LON);
  prefs.end();

  mqttHost = OBSERVER_MQTT_HOST;
  mqttPort = OBSERVER_MQTT_PORT;
  mqttUser = OBSERVER_MQTT_USER;
  mqttPass = OBSERVER_MQTT_PASS;

  if (observerId.length() == 0) observerId = macId();
  if (observerName.length() == 0) observerName = observerId;
}

static inline void renderDisplay() {
  if (!displayReady) return;
  display.clearDisplay();
  display.setTextSize(1);
  display.setTextColor(SSD1306_WHITE);
  display.setCursor(0, 0);
  display.println("MeshRank Observer");
  display.setCursor(0, 12);
  display.print("Name: ");
  display.println(observerName.length() ? observerName : "-");

  display.setCursor(0, 24);
  display.print("WiFi: ");
  if (WiFi.status() == WL_CONNECTED) {
    display.print(wifiSsid.length() ? wifiSsid : "connected");
  } else if (wifiSsid.length()) {
    display.print("connecting");
  } else {
    display.print("not set");
  }

  display.setCursor(0, 36);
  display.print("IP: ");
  if (WiFi.status() == WL_CONNECTED) {
    display.print(WiFi.localIP().toString());
  } else {
    display.print("--");
  }

  display.setCursor(0, 48);
  display.print("MQTT: ");
  display.println(mqttClient.connected() ? "connected" : "offline");
  display.display();
}

static inline bool probeI2c(uint8_t addr) {
  Wire.beginTransmission(addr);
  return Wire.endTransmission() == 0;
}

static inline void setVext(bool enabled) {
  pinMode(VEXT_EN, OUTPUT);
  if (vextActiveLow) {
    digitalWrite(VEXT_EN, enabled ? LOW : HIGH);
  } else {
    digitalWrite(VEXT_EN, enabled ? HIGH : LOW);
  }
}

static inline uint8_t scanI2c(String &out) {
  out = "";
  uint8_t first = 0;
  for (uint8_t addr = 0x03; addr <= 0x77; addr++) {
    if (!probeI2c(addr)) continue;
    if (!first) first = addr;
    char buf[8];
    snprintf(buf, sizeof(buf), "0x%02X", addr);
    if (out.length()) out += " ";
    out += buf;
  }
  return first;
}

static inline void saveConfig() {
  prefs.begin(PREFS_NS, false);
  prefs.putString("ssid", wifiSsid);
  prefs.putString("pass", wifiPass);
  prefs.putString("id", observerId);
  prefs.putString("name", observerName);
  prefs.putFloat("lat", observerLat);
  prefs.putFloat("lon", observerLon);
  prefs.end();
}

static inline bool spoolAppend(const String &line) {
  if (!SPIFFS.begin(true)) return false;
  File f = SPIFFS.open(SPOOL_PATH, FILE_APPEND);
  if (!f) return false;
  f.println(line);
  f.close();
  File stat = SPIFFS.open(SPOOL_PATH, FILE_READ);
  if (stat && stat.size() > MAX_SPOOL_BYTES) {
    stat.close();
    SPIFFS.remove(SPOOL_PATH);
  } else if (stat) {
    stat.close();
  }
  return true;
}

static inline void spoolFlush() {
  if (!SPIFFS.begin(true)) return;
  if (!SPIFFS.exists(SPOOL_PATH)) return;
  File f = SPIFFS.open(SPOOL_PATH, FILE_READ);
  if (!f) return;
  while (f.available()) {
    String line = f.readStringUntil('\n');
    line.trim();
    if (line.length() == 0) continue;
    if (!mqttClient.connected()) break;
    mqttClient.publish(String("meshrank/observers/" + observerId + "/packets").c_str(), line.c_str());
    delay(2);
  }
  f.close();
  if (mqttClient.connected()) {
    SPIFFS.remove(SPOOL_PATH);
  }
}

// ================= SERIAL CONFIG =================
static inline void handleSerialConfig() {
#if OBSERVER_SERIAL_CONFIG
  static String buffer;
  while (Serial.available()) {
    char c = (char)Serial.read();
    if (c == '\n' || c == '\r') {
      buffer.trim();
      if (buffer.length() == 0) continue;
      if (buffer.startsWith("wifi.ssid ")) {
        wifiSsid = buffer.substring(10);
        saveConfig();
        displayDirty = true;
        Serial.println("[observer] cfg ssid updated");
      } else if (buffer.startsWith("wifi.pass ")) {
        wifiPass = buffer.substring(10);
        saveConfig();
        displayDirty = true;
        Serial.println("[observer] cfg pass updated");
      } else if (buffer.startsWith("mqtt.")) {
        // MQTT endpoint is fixed (TLS-only).
      } else if (buffer.startsWith("observer.lat ")) {
        observerLat = buffer.substring(13).toFloat();
        saveConfig();
        displayDirty = true;
        Serial.println("[observer] cfg lat updated");
      } else if (buffer.startsWith("observer.lon ")) {
        observerLon = buffer.substring(13).toFloat();
        saveConfig();
        displayDirty = true;
        Serial.println("[observer] cfg lon updated");
      } else if (buffer.startsWith("observer.name ")) {
        observerName = buffer.substring(14);
        saveConfig();
        displayDirty = true;
        Serial.println("[observer] cfg name updated");
      } else if (buffer == "status") {
        Serial.println("{\"ok\":true,\"fw\":\"" OBSERVER_FW_VER "\",\"ssid\":\"" + wifiSsid + "\",\"host\":\"" + mqttHost + "\",\"port\":" + String(mqttPort) + ",\"id\":\"" + observerId + "\",\"name\":\"" + observerName + "\",\"lat\":" + String(observerLat, 6) + ",\"lon\":" + String(observerLon, 6) + "}");
      }
      buffer = "";
      continue;
    }
    buffer += c;
  }
#endif
}

// ================= SETUP =================
void setup() {
  Serial.begin(115200);
  delay(400);

  loadConfig();
  Serial.println("[observer] boot");
  Serial.println(String("[observer] fw=") + OBSERVER_FW_VER);
  Serial.print("[observer] ssid=");
  Serial.println(wifiSsid.length() ? wifiSsid : "<empty>");

  setVext(true);
  Wire.begin(OLED_SDA, OLED_SCL);
  Wire.setClock(400000);
  delay(50);

  String found;
  uint8_t addr = scanI2c(found);
  if (!addr) {
    vextActiveLow = false;
    setVext(true);
    delay(50);
    addr = scanI2c(found);
  }

  if (found.length()) {
    Serial.print("[observer] i2c devices: ");
    Serial.println(found);
  } else {
    Serial.println("[observer] i2c scan empty");
  }

  if (addr) {
    oledAddr = addr;
  }

  if (addr && display.begin(SSD1306_SWITCHCAPVCC, oledAddr)) {
    displayReady = true;
    displayDirty = true;
    renderDisplay();
    Serial.print("[observer] oled ok addr=");
    Serial.println(oledAddr, HEX);
  } else if (addr) {
    Serial.print("[observer] oled init failed addr=");
    Serial.println(oledAddr, HEX);
  } else {
    Serial.println("[observer] oled not detected");
  }

  WiFi.mode(WIFI_STA);
  if (wifiSsid.length()) {
    WiFi.begin(wifiSsid.c_str(), wifiPass.c_str());
  }

  tlsClient.setInsecure();
  mqttClient.setServer(mqttHost.c_str(), mqttPort);
  mqttClient.setBufferSize(MQTT_BUFFER_SIZE);

  SPI.begin(LORA_SCK, LORA_MISO, LORA_MOSI, LORA_CS);
  radio.setTCXO(0.0);
  radio.setCRC(true);
  radio.setSyncWord(0x12);
  int state = radio.begin(FREQ_MHZ, BW_KHZ, SF, CR_DENOM);
  if (state != RADIOLIB_ERR_NONE) {
    while (true) delay(1000);
  }
  radio.setDio1Action(onDio1);
  radio.startReceive();
}

// ================= LOOP =================
void loop() {
  handleSerialConfig();

  if (WiFi.status() == WL_CONNECTED && !wifiWasConnected) {
    wifiWasConnected = true;
    Serial.print("[observer] wifi connected ip=");
    Serial.println(WiFi.localIP());
    displayDirty = true;
  }

  if (WiFi.status() != WL_CONNECTED && wifiWasConnected) {
    wifiWasConnected = false;
    Serial.println("[observer] wifi disconnected");
    displayDirty = true;
  }

  if (WiFi.status() == WL_CONNECTED && !mqttClient.connected()) {
    String clientId = "obs-" + observerId;
    if (mqttUser.length()) {
      mqttClient.connect(clientId.c_str(), mqttUser.c_str(), mqttPass.c_str());
    } else {
      mqttClient.connect(clientId.c_str());
    }
    if (mqttClient.connected()) {
      if (!mqttWasConnected) {
        mqttWasConnected = true;
        Serial.print("[observer] mqtt connected ");
        Serial.print(mqttHost);
        Serial.print(":");
        Serial.println(mqttPort);
        displayDirty = true;
      }
      spoolFlush();
    }
  }
  if (!mqttClient.connected() && mqttWasConnected) {
    mqttWasConnected = false;
    Serial.println("[observer] mqtt disconnected");
    displayDirty = true;
  }
  mqttClient.loop();

  if (displayReady && (displayDirty || millis() - lastDisplayMs > 3000)) {
    renderDisplay();
    displayDirty = false;
    lastDisplayMs = millis();
  }

  if (!takeRxFlag()) {
    delay(2);
    return;
  }

  uint8_t buf[255];
  int reportedLen = radio.getPacketLength();
  int len = reportedLen;
  if (len <= 0) len = (int)sizeof(buf);
  if (len > (int)sizeof(buf)) len = sizeof(buf);

  float rssi = radio.getRSSI();
  float snr = radio.getSNR();
  int state = radio.readData(buf, len);
  int ptype = (len > 0) ? buf[0] : -1;
  Serial.printf("[observer] rx len=%d rssi=%.1f snr=%.2f crc=%s\n",
                len, rssi, snr, (state == RADIOLIB_ERR_NONE ? "ok" : "bad"));

  String frameHash = sha256Hex(buf, len);
  char payloadHex[512];
  if ((size_t)len * 2 >= sizeof(payloadHex)) len = (sizeof(payloadHex) / 2) - 1;
  toHex(buf, len, payloadHex);

  String json = String("{\"observerId\":\"") + observerId +
                "\",\"observerName\":\"" + observerName +
                "\",\"ts\":" + String(millis()) +
                ",\"ptype\":" + String(ptype) +
                ",\"crc\":" + String(state == RADIOLIB_ERR_NONE ? "true" : "false") +
                ",\"rssi\":" + String(rssi, 1) +
                ",\"snr\":" + String(snr, 2) +
                ",\"reported_len\":" + String(reportedLen) +
                ",\"len\":" + String(len) +
                ",\"payloadHex\":\"" + String(payloadHex) +
                "\",\"frameHash\":\"" + frameHash + "\"";
  if (observerLat != 0.0f || observerLon != 0.0f) {
    json += ",\"gps\":{\"lat\":" + String(observerLat, 6) + ",\"lon\":" + String(observerLon, 6) + "}";
  }
  json += "}";

  if (mqttClient.connected()) {
    if (!mqttClient.publish(String("meshrank/observers/" + observerId + "/packets").c_str(), json.c_str())) {
      Serial.printf("[observer] mqtt publish failed len=%d\n", json.length());
    }
  } else {
    spoolAppend(json);
  }

  radio.startReceive();
  delay(2);
}
