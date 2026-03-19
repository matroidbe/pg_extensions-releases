//! MQTT 5.0 packet type definitions
//!
//! Defines the structures for all MQTT control packets.

/// MQTT Control Packet Types (4-bit values in the fixed header)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketType {
    Connect = 1,
    Connack = 2,
    Publish = 3,
    Puback = 4,
    Pubrec = 5,
    Pubrel = 6,
    Pubcomp = 7,
    Subscribe = 8,
    Suback = 9,
    Unsubscribe = 10,
    Unsuback = 11,
    Pingreq = 12,
    Pingresp = 13,
    Disconnect = 14,
    Auth = 15,
}

impl TryFrom<u8> for PacketType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(PacketType::Connect),
            2 => Ok(PacketType::Connack),
            3 => Ok(PacketType::Publish),
            4 => Ok(PacketType::Puback),
            5 => Ok(PacketType::Pubrec),
            6 => Ok(PacketType::Pubrel),
            7 => Ok(PacketType::Pubcomp),
            8 => Ok(PacketType::Subscribe),
            9 => Ok(PacketType::Suback),
            10 => Ok(PacketType::Unsubscribe),
            11 => Ok(PacketType::Unsuback),
            12 => Ok(PacketType::Pingreq),
            13 => Ok(PacketType::Pingresp),
            14 => Ok(PacketType::Disconnect),
            15 => Ok(PacketType::Auth),
            _ => Err(()),
        }
    }
}

/// MQTT QoS levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum QoS {
    #[default]
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl TryFrom<u8> for QoS {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(QoS::AtMostOnce),
            1 => Ok(QoS::AtLeastOnce),
            2 => Ok(QoS::ExactlyOnce),
            _ => Err(()),
        }
    }
}

/// MQTT 5.0 Reason Codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ReasonCode {
    Success = 0x00,
    // GrantedQoS0 is same as Success (0x00) - use Success for QoS 0
    GrantedQoS1 = 0x01,
    GrantedQoS2 = 0x02,
    DisconnectWithWillMessage = 0x04,
    NoMatchingSubscribers = 0x10,
    NoSubscriptionExisted = 0x11,
    UnspecifiedError = 0x80,
    MalformedPacket = 0x81,
    ProtocolError = 0x82,
    ImplementationSpecificError = 0x83,
    UnsupportedProtocolVersion = 0x84,
    ClientIdentifierNotValid = 0x85,
    BadUserNameOrPassword = 0x86,
    NotAuthorized = 0x87,
    ServerUnavailable = 0x88,
    ServerBusy = 0x89,
    Banned = 0x8A,
    BadAuthenticationMethod = 0x8C,
    TopicNameInvalid = 0x90,
    PacketTooLarge = 0x95,
    QuotaExceeded = 0x97,
    PayloadFormatInvalid = 0x99,
    RetainNotSupported = 0x9A,
    QoSNotSupported = 0x9B,
    UseAnotherServer = 0x9C,
    ServerMoved = 0x9D,
    ConnectionRateExceeded = 0x9F,
}

impl ReasonCode {
    /// Alias for Success when used as normal disconnection
    pub fn normal_disconnection() -> Self {
        ReasonCode::Success
    }
}

impl From<ReasonCode> for u8 {
    fn from(code: ReasonCode) -> u8 {
        code as u8
    }
}

/// CONNECT packet from client
#[derive(Debug, Clone)]
pub struct ConnectPacket {
    /// Protocol name (must be "MQTT")
    pub protocol_name: String,
    /// Protocol version (5 for MQTT 5.0)
    pub protocol_version: u8,
    /// Connect flags
    pub flags: ConnectFlags,
    /// Keep alive interval in seconds
    pub keep_alive: u16,
    /// MQTT 5.0 properties
    pub properties: ConnectProperties,
    /// Client identifier
    pub client_id: String,
    /// Will properties (if will flag set)
    pub will_properties: Option<WillProperties>,
    /// Will topic (if will flag set)
    pub will_topic: Option<String>,
    /// Will payload (if will flag set)
    pub will_payload: Option<Vec<u8>>,
    /// Username (if username flag set)
    pub username: Option<String>,
    /// Password (if password flag set)
    pub password: Option<Vec<u8>>,
}

/// Connect flags byte
#[derive(Debug, Clone, Default)]
pub struct ConnectFlags {
    pub clean_start: bool,
    pub will_flag: bool,
    pub will_qos: QoS,
    pub will_retain: bool,
    pub password_flag: bool,
    pub username_flag: bool,
}

impl ConnectFlags {
    pub fn from_byte(byte: u8) -> Self {
        Self {
            clean_start: (byte & 0x02) != 0,
            will_flag: (byte & 0x04) != 0,
            will_qos: QoS::try_from((byte >> 3) & 0x03).unwrap_or_default(),
            will_retain: (byte & 0x20) != 0,
            password_flag: (byte & 0x40) != 0,
            username_flag: (byte & 0x80) != 0,
        }
    }
}

/// MQTT 5.0 CONNECT properties
#[derive(Debug, Clone, Default)]
pub struct ConnectProperties {
    pub session_expiry_interval: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub maximum_packet_size: Option<u32>,
    pub topic_alias_maximum: Option<u16>,
    pub request_response_information: Option<bool>,
    pub request_problem_information: Option<bool>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Vec<u8>>,
}

/// Will properties
#[derive(Debug, Clone, Default)]
pub struct WillProperties {
    pub will_delay_interval: Option<u32>,
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Vec<u8>>,
}

/// CONNACK packet to client
#[derive(Debug, Clone)]
pub struct ConnackPacket {
    /// Session present flag
    pub session_present: bool,
    /// Reason code
    pub reason_code: ReasonCode,
    /// Properties
    pub properties: ConnackProperties,
}

/// CONNACK properties
#[derive(Debug, Clone, Default)]
pub struct ConnackProperties {
    pub session_expiry_interval: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub maximum_qos: Option<u8>,
    pub retain_available: Option<bool>,
    pub maximum_packet_size: Option<u32>,
    pub assigned_client_identifier: Option<String>,
    pub topic_alias_maximum: Option<u16>,
    pub wildcard_subscription_available: Option<bool>,
    pub subscription_identifier_available: Option<bool>,
    pub shared_subscription_available: Option<bool>,
    pub server_keep_alive: Option<u16>,
}

/// PUBLISH packet
#[derive(Debug, Clone)]
pub struct PublishPacket {
    /// DUP flag
    pub dup: bool,
    /// QoS level
    pub qos: QoS,
    /// Retain flag
    pub retain: bool,
    /// Topic name
    pub topic: String,
    /// Packet identifier (only for QoS > 0)
    pub packet_id: Option<u16>,
    /// Properties
    pub properties: PublishProperties,
    /// Payload
    pub payload: Vec<u8>,
}

/// PUBLISH properties
#[derive(Debug, Clone, Default)]
pub struct PublishProperties {
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub topic_alias: Option<u16>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Vec<u8>>,
    pub content_type: Option<String>,
}

/// SUBSCRIBE packet
#[derive(Debug, Clone)]
pub struct SubscribePacket {
    /// Packet identifier
    pub packet_id: u16,
    /// Properties
    pub properties: SubscribeProperties,
    /// Topic filters with subscription options
    pub subscriptions: Vec<SubscriptionRequest>,
}

/// SUBSCRIBE properties
#[derive(Debug, Clone, Default)]
pub struct SubscribeProperties {
    pub subscription_identifier: Option<u32>,
}

/// Individual subscription request
#[derive(Debug, Clone)]
pub struct SubscriptionRequest {
    /// Topic filter (may contain + and # wildcards)
    pub topic_filter: String,
    /// Subscription options
    pub options: SubscriptionOptions,
}

/// Subscription options byte
#[derive(Debug, Clone, Default)]
pub struct SubscriptionOptions {
    pub qos: QoS,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: u8,
}

impl SubscriptionOptions {
    pub fn from_byte(byte: u8) -> Self {
        Self {
            qos: QoS::try_from(byte & 0x03).unwrap_or_default(),
            no_local: (byte & 0x04) != 0,
            retain_as_published: (byte & 0x08) != 0,
            retain_handling: (byte >> 4) & 0x03,
        }
    }
}

/// SUBACK packet
#[derive(Debug, Clone)]
pub struct SubackPacket {
    /// Packet identifier
    pub packet_id: u16,
    /// Properties
    pub properties: SubackProperties,
    /// Reason codes for each subscription
    pub reason_codes: Vec<ReasonCode>,
}

/// SUBACK properties
#[derive(Debug, Clone, Default)]
pub struct SubackProperties {
    pub reason_string: Option<String>,
}

/// UNSUBSCRIBE packet
#[derive(Debug, Clone)]
pub struct UnsubscribePacket {
    /// Packet identifier
    pub packet_id: u16,
    /// Properties
    pub properties: UnsubscribeProperties,
    /// Topic filters to unsubscribe from
    pub topic_filters: Vec<String>,
}

/// UNSUBSCRIBE properties
#[derive(Debug, Clone, Default)]
pub struct UnsubscribeProperties {}

/// UNSUBACK packet
#[derive(Debug, Clone)]
pub struct UnsubackPacket {
    /// Packet identifier
    pub packet_id: u16,
    /// Properties
    pub properties: UnsubackProperties,
    /// Reason codes for each unsubscription
    pub reason_codes: Vec<ReasonCode>,
}

/// UNSUBACK properties
#[derive(Debug, Clone, Default)]
pub struct UnsubackProperties {
    pub reason_string: Option<String>,
}

/// DISCONNECT packet
#[derive(Debug, Clone)]
pub struct DisconnectPacket {
    /// Reason code
    pub reason_code: ReasonCode,
    /// Properties
    pub properties: DisconnectProperties,
}

/// DISCONNECT properties
#[derive(Debug, Clone, Default)]
pub struct DisconnectProperties {
    pub session_expiry_interval: Option<u32>,
    pub reason_string: Option<String>,
    pub server_reference: Option<String>,
}

/// Parsed MQTT packet
#[derive(Debug, Clone)]
pub enum MqttPacket {
    Connect(ConnectPacket),
    Connack(ConnackPacket),
    Publish(PublishPacket),
    Puback { packet_id: u16 },
    Pubrec { packet_id: u16 },
    Pubrel { packet_id: u16 },
    Pubcomp { packet_id: u16 },
    Subscribe(SubscribePacket),
    Suback(SubackPacket),
    Unsubscribe(UnsubscribePacket),
    Unsuback(UnsubackPacket),
    Pingreq,
    Pingresp,
    Disconnect(DisconnectPacket),
    Auth,
}
