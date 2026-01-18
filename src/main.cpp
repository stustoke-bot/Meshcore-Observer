// src/main.cpp
#include <Arduino.h>
#include <SPI.h>
#include <RadioLib.h>

// ================= PIN MAP (Heltec WiFi LoRa 32 V3 / V3.2) =================
#define LORA_CS    8
#define LORA_SCK   9
#define LORA_MOSI  10
#define LORA_MISO  11
#define LORA_RST   12
#define LORA_BUSY  13
#define LORA_DIO1  14

// ================= RF PARAMETERS (your working settings) =================
#define FREQ_MHZ   869.618
#define BW_KHZ     62.5
#define SF         8
#define CR_DENOM   8        // 4/8

// ================= RADIO INSTANCE =================
SX1262 radio = new Module(LORA_CS, LORA_DIO1, LORA_RST, LORA_BUSY);
volatile bool rxFlag = false;

// ================= ISR =================
void IRAM_ATTR onDio1() {
  rxFlag = true;
}

// ================= UTILITIES =================
static inline void printHex(const uint8_t* data, size_t len) {
  for (size_t i = 0; i < len; i++) {
    if (data[i] < 0x10) Serial.print('0');
    Serial.print(data[i], HEX);
  }
}

// Stable fingerprint (FNV-1a over first N bytes)
static inline uint64_t fnv1a64(const uint8_t* data, size_t len) {
  uint64_t h = 0xcbf29ce484222325ULL;
  for (size_t i = 0; i < len; i++) {
    h ^= data[i];
    h *= 0x100000001b3ULL;
  }
  return h;
}

static inline bool takeRxFlag() {
  bool got;
  noInterrupts();
  got = rxFlag;
  rxFlag = false;
  interrupts();
  return got;
}

// ================= SETUP =================
void setup() {
  Serial.begin(115200);
  delay(1200);

  Serial.println();
  Serial.println("=== Heltec V3.2 MeshCORE Deep RF Sniffer ===");
  Serial.println("Mode: CRC ON | Syncword 0x12 | Fingerprint enabled");

  SPI.begin(LORA_SCK, LORA_MISO, LORA_MOSI, LORA_CS);

  // Heltec V3/V3.2 often has a TCXO; if your 0.0 works, keep it.
  // If you see flaky RX, try 1.8 (common) instead.
  radio.setTCXO(0.0);

  // Tighten to MeshCORE PHY
  radio.setCRC(true);
  radio.setSyncWord(0x12);

  int state = radio.begin(FREQ_MHZ, BW_KHZ, SF, CR_DENOM);
  if (state != RADIOLIB_ERR_NONE) {
    Serial.print("radio.begin FAILED: ");
    Serial.println(state);
    while (true) delay(1000);
  }

  radio.setDio1Action(onDio1);
  radio.startReceive();

  Serial.println("Radio initialised OK");
  Serial.println("Listening...");
}

// ================= LOOP =================
void loop() {
  if (!takeRxFlag()) {
    delay(2);
    return;
  }

  uint8_t buf[255];

  // Capture what the radio *thinks* length is (can be 0 depending on timing/modem state)
  int reportedLen = radio.getPacketLength();

  // Safe clamp for read length
  int len = reportedLen;
  if (len <= 0) len = (int)sizeof(buf);          // fallback: attempt read (we still report reportedLen)
  if (len > (int)sizeof(buf)) len = sizeof(buf); // clamp

  // Read signal stats around RX completion (some libs are more stable here)
  float rssi = radio.getRSSI();
  float snr  = radio.getSNR();

  // Read data
  int state = radio.readData(buf, len);

  // Frame type (first byte) if present
  int ptype = (len > 0) ? buf[0] : -1;

  // Fingerprint first 20 bytes (or less)
  int fpLen = min(len, 20);
  uint64_t fp = fnv1a64(buf, fpLen);

  // ================= JSON OUTPUT =================
  Serial.print("{\"type\":\"rf\"");

  // Timestamp (ms since boot)
  Serial.print(",\"ts\":");
  Serial.print(millis());

  // Packet type byte (useful for MeshCORE 0x11 adverts / 0x15 grouptext, etc.)
  Serial.print(",\"ptype\":");
  Serial.print(ptype);

  // Fingerprint
  Serial.print(",\"fp\":\"");
  Serial.printf("%016llX", fp);
  Serial.print("\"");

  // RadioLib state code + boolean CRC result
  Serial.print(",\"state\":");
  Serial.print(state);

  Serial.print(",\"crc\":");
  Serial.print(state == RADIOLIB_ERR_NONE ? "true" : "false");

  // RF stats
  Serial.print(",\"rssi\":");
  Serial.print(rssi, 1);

  Serial.print(",\"snr\":");
  Serial.print(snr, 2);

  // Lengths (reported vs used)
  Serial.print(",\"reported_len\":");
  Serial.print(reportedLen);

  Serial.print(",\"len\":");
  Serial.print(len);

  // Payload hex
  Serial.print(",\"hex\":\"");
  printHex(buf, len);
  Serial.println("\"}");

  // Resume RX immediately
  radio.startReceive();
  delay(2);
}
