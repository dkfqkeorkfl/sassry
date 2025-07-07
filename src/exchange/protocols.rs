use std::{
    collections::{HashMap, VecDeque},
    fmt,
    hash::{Hash, Hasher},
    str::FromStr,
    sync::Arc,
};

use super::super::webserver::websocket::*;
use cassry::{
    secrecy::SecretString, util::{
        deserialize_anyhow_error, deserialize_chrono_duration, serialize_anyhow_error,
        serialize_chrono_duration,
    }, *
};
use chrono::prelude::*;
use futures::FutureExt;
use rust_decimal::{prelude::*, Decimal};
use serde::{
    de::{self, SeqAccess, Visitor},
    ser::SerializeTuple,
    Deserialize, Deserializer, Serialize, Serializer,
};
use serde_json::json;
use tokio::sync::RwLock;

fn serialize_error_map<S>(
    errors: &HashMap<String, anyhow::Error>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let errors_as_strings = errors
        .iter()
        .map(|(k, v)| (k.clone(), v.to_string()))
        .collect::<HashMap<_, _>>();
    errors_as_strings.serialize(serializer)
}

fn deserialize_error_map<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, anyhow::Error>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let errors_as_strings = HashMap::<String, String>::deserialize(deserializer)?;
    Ok(errors_as_strings
        .into_iter()
        .map(|(k, v)| (k, anyhowln!("{}", v)))
        .collect())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum SubscribeSpeed {
    #[default]
    None,
    Fastest,
    Slowest,
    Least(String),
    Fixed(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum SubscribeQuantity {
    #[default]
    None,
    Much,
    Few,
    Least(String),
    Fixed(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum OrderBookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default, Hash)]
pub enum OrderSide {
    Buy,
    #[default]
    Sell,
}

impl OrderSide {
    pub fn is_buy(&self) -> bool {
        match self {
            OrderSide::Buy => true,
            OrderSide::Sell => false,
        }
    }

    pub fn is_sell(&self) -> bool {
        self.is_buy() == false
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum MarketOpt {
    Spot,
    Margin,
    Derivatives,
    InverseFuture,
    InversePerpetual,
    LinearFuture,
    LinearPerpetual,
    #[default]
    All,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum MarketKind {
    Spot(String),
    Margin(String),
    Derivatives(String),
    LinearFuture(String),
    LinearPerpetual(String),
    InverseFuture(String),
    InversePerpetual(String),
}

impl MarketKind {
    pub fn symbol(&self) -> &str {
        match self {
            MarketKind::Spot(s) => s.as_str(),
            MarketKind::Margin(s) => s.as_str(),
            MarketKind::Derivatives(s) => s.as_str(),
            MarketKind::LinearFuture(s) => s.as_str(),
            MarketKind::LinearPerpetual(s) => s.as_str(),
            MarketKind::InverseFuture(s) => s.as_str(),
            MarketKind::InversePerpetual(s) => s.as_str(),
        }
    }
}

impl Serialize for MarketKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_tuple(2)?;
        match self {
            MarketKind::Spot(data) => {
                state.serialize_element(data)?;
                state.serialize_element("Spot")?;
            }
            MarketKind::Margin(data) => {
                state.serialize_element(data)?;
                state.serialize_element("Margin")?;
            }
            MarketKind::Derivatives(data) => {
                state.serialize_element(data)?;
                state.serialize_element("Derivatives")?;
            }
            MarketKind::LinearFuture(data) => {
                state.serialize_element(data)?;
                state.serialize_element("LinearFuture")?;
            }
            MarketKind::LinearPerpetual(data) => {
                state.serialize_element(data)?;
                state.serialize_element("LinearPerpetual")?;
            }
            MarketKind::InverseFuture(data) => {
                state.serialize_element(data)?;
                state.serialize_element("InverseFuture")?;
            }
            MarketKind::InversePerpetual(data) => {
                state.serialize_element(data)?;
                state.serialize_element("InversePerpetual")?;
            }
        }
        state.end()
    }
}

impl<'de> Deserialize<'de> for MarketKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MarketKindVisitor;

        impl<'de> Visitor<'de> for MarketKindVisitor {
            type Value = MarketKind;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a tuple representing a MarketKind")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let data: String = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let kind: String = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;

                match kind.as_str() {
                    "Spot" => Ok(MarketKind::Spot(data)),
                    "Margin" => Ok(MarketKind::Margin(data)),
                    "Derivatives" => Ok(MarketKind::Derivatives(data)),
                    "LinearFuture" => Ok(MarketKind::LinearFuture(data)),
                    "LinearPerpetual" => Ok(MarketKind::LinearPerpetual(data)),
                    "InverseFuture" => Ok(MarketKind::InverseFuture(data)),
                    "InversePerpetual" => Ok(MarketKind::InversePerpetual(data)),
                    _ => Err(de::Error::unknown_variant(&kind, VARIANTS)),
                }
            }
        }

        const VARIANTS: &'static [&'static str] = &[
            "Spot",
            "Margin",
            "Derivatives",
            "LinearFuture",
            "LinearPerpetual",
            "InverseFuture",
            "InversePerpetual",
        ];
        deserializer.deserialize_tuple(2, MarketKindVisitor)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum CurrencySide {
    #[default]
    None,
    Quote,
    Base,
    Specific(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TickRange(Decimal, Decimal);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PrecisionKind {
    RangeTick(Vec<TickRange>),
    Tick(Decimal),
}

impl PrecisionKind {
    pub fn calculate_with_range_impl(
        val: &Decimal,
        side: &OrderBookSide,
        lvl: u64,
        range: &Vec<TickRange>,
    ) -> anyhow::Result<Decimal> {
        let idx = range
            .binary_search_by(|r| r.0.cmp(val))
            .unwrap_or_else(|e| e - 1);
        if range[idx].1 == Decimal::ZERO {
            return Err(anyhowln!("tick is Zero."));
        }

        let tick = range[idx].1;

        if OrderBookSide::Ask == *side {
            let nxt_range = if let Some(nxt) = range.get(idx + 1) {
                nxt.0
            } else {
                Decimal::MAX
            };

            let sz = ((nxt_range - val) / tick).to_u64().unwrap();
            let fixed_lvl = lvl - sz.min(lvl);
            if fixed_lvl == 0 {
                return Ok(val + (tick * Decimal::from(lvl)));
            }

            return PrecisionKind::calculate_with_range_impl(&nxt_range, side, fixed_lvl, range);
        }

        let sz = ((val - range[idx].0) / tick).to_u64().unwrap();
        let fixed_lvl = lvl - sz.min(lvl);
        if fixed_lvl == 0 {
            return Ok(val - (tick * Decimal::from(lvl)));
        }

        return PrecisionKind::calculate_with_range_impl(&range[idx].0, side, fixed_lvl, range);
    }

    pub fn calculate_with_range(
        val: &Decimal,
        side: &OrderBookSide,
        lvl: i64,
        range: &Vec<TickRange>,
    ) -> anyhow::Result<Decimal> {
        if lvl < 0 {
            return PrecisionKind::calculate_with_range(
                val,
                if OrderBookSide::Ask == *side {
                    &OrderBookSide::Bid
                } else {
                    &OrderBookSide::Ask
                },
                lvl.abs(),
                range,
            );
        }

        let idx = range
            .binary_search_by(|r| r.0.cmp(val))
            .unwrap_or_else(|e| e - 1);
        if range[idx].1 == Decimal::ZERO {
            return Err(anyhowln!("tick is Zero."));
        }

        let tick = range[idx].1;
        let weight = val / tick;
        let fixed = if OrderBookSide::Ask == *side {
            weight.ceil()
        } else {
            weight.floor()
        } * tick;

        return PrecisionKind::calculate_with_range_impl(&fixed, side, lvl as u64, range);
    }

    pub fn calculate_with_tick(
        val: &Decimal,
        side: &OrderBookSide,
        lvl: i64,
        tick: &Decimal,
    ) -> anyhow::Result<Decimal> {
        if tick == &Decimal::ZERO {
            return Err(anyhowln!("tick is Zero."));
        }

        let weight = val / tick;
        let fixed = if OrderBookSide::Ask == *side {
            weight.ceil()
        } else {
            weight.floor()
        } * tick;

        let d = Decimal::from(if OrderBookSide::Ask == *side {
            lvl
        } else {
            -lvl
        });
        return Ok(fixed + (tick * d));
    }

    pub fn calculate_price(
        &self,
        val: &Decimal,
        side: &OrderBookSide,
        lvl: i64,
    ) -> anyhow::Result<Decimal> {
        match self {
            PrecisionKind::Tick(tick) => PrecisionKind::calculate_with_tick(val, side, lvl, tick),
            PrecisionKind::RangeTick(ranges) => {
                PrecisionKind::calculate_with_range(val, side, lvl, ranges)
            }
        }
    }

    pub fn calculate_amount(
        &self,
        _price: &Decimal,
        amount: &Decimal,
        lvl: i64,
    ) -> anyhow::Result<Decimal> {
        match self {
            PrecisionKind::Tick(tick) => {
                PrecisionKind::calculate_with_tick(amount, &OrderBookSide::Bid, lvl, tick)
            }
            PrecisionKind::RangeTick(ranges) => {
                PrecisionKind::calculate_with_range(amount, &OrderBookSide::Bid, lvl, ranges)
            }
        }
    }
}

impl Default for PrecisionKind {
    fn default() -> Self {
        PrecisionKind::Tick(Decimal::ZERO)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MarketState {
    Work,
    Hidden,
    Disable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum OrderKind {
    #[default]
    Limit,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum OrderState {
    Ordering(chrono::DateTime<Utc> /* ordered time */),
    Opened,
    Canceling(chrono::DateTime<Utc> /* ordered time */),
    PartiallyFilled,
    Filled,
    #[default]
    Rejected,
    Cancelled,
}

impl OrderState {
    pub fn is_process(&self) -> bool {
        match self {
            OrderState::Ordering(_)
            | OrderState::Opened
            | OrderState::PartiallyFilled
            | OrderState::Canceling(_) => true,
            _ => false,
        }
    }
    pub fn is_cancelled(&self) -> bool {
        match self {
            OrderState::Rejected | OrderState::Cancelled => true,
            _ => false,
        }
    }
    pub fn is_done(&self) -> bool {
        match self {
            OrderState::Filled => true,
            _ => false,
        }
    }

    pub fn synchronizable(&self) -> bool {
        match self {
            OrderState::Rejected | OrderState::Cancelled | OrderState::Filled => false,
            _ => true,
        }
    }

    pub fn cancelable(&self) -> bool {
        match self {
            OrderState::Ordering(_) | OrderState::Opened | OrderState::PartiallyFilled => true,
            _ => false,
        }
    }

    pub fn get_created(&self) -> Option<&DateTime<Utc>> {
        match self {
            OrderState::Ordering(created) | OrderState::Canceling(created) => Some(&created),
            _ => None,
        }
    }

    pub fn is_ordering_or_canceling(&self) -> bool {
        match self {
            OrderState::Ordering(_created) | OrderState::Canceling(_created) => true,
            _ => false,
        }
    }
    pub fn is_expired(&self, expired: &chrono::Duration) -> bool {
        if let Some(created) = self.get_created() {
            Utc::now() - *created > *expired
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PacketTime {
    pub sendtime: chrono::DateTime<Utc>,
    pub recvtime: chrono::DateTime<Utc>,
}

impl From<DateTime<Utc>> for PacketTime {
    fn from(dt: DateTime<Utc>) -> Self {
        PacketTime {
            sendtime: dt.clone(),
            recvtime: dt, // 현재 시간으로 recvtime 설정
        }
    }
}

impl Default for PacketTime {
    fn default() -> Self {
        Self {
            sendtime: Utc::now(),
            recvtime: Utc::now(),
        }
    }
}

impl PacketTime {
    pub fn new(time: &chrono::DateTime<Utc>) -> Self {
        Self {
            sendtime: time.clone(),
            recvtime: time.clone(),
        }
    }

    pub fn laytency(&self) -> chrono::Duration {
        self.recvtime - self.sendtime
    }
}

pub trait UpdatedTrait {
    fn get_updated(&self) -> &DateTime<Utc>;
}

pub trait KeyTrait<T> {
    fn get_key(&self) -> &T;
}

pub trait PacketTimeTrait {
    fn get_expired_date(&self, dur: &chrono::Duration) -> chrono::DateTime<Utc> {
        self.get_packet_time().recvtime + *dur
    }

    fn get_packet_time(&self) -> &PacketTime;
    fn laytency(&self) -> chrono::Duration {
        self.get_packet_time().laytency()
    }
}

#[repr(u32)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum UpdateType {
    Snapshot = 1,
    Partial = 2,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum CursorType {
    #[default]
    None,
    MinMax(usize, usize),
    PrevNxt(String, String),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FeeInfos {
    bm: CurrencyPair,
    bt: CurrencyPair,
    sm: CurrencyPair,
    st: CurrencyPair,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CurrencyPair(CurrencySide, Decimal);

impl CurrencyPair {
    pub fn new(c: CurrencySide, v: Decimal) -> Self {
        CurrencyPair { 0: c, 1: v }
    }

    pub fn new_base(v: Decimal) -> Self {
        CurrencyPair {
            0: CurrencySide::Base,
            1: v,
        }
    }

    pub fn new_quote(v: Decimal) -> Self {
        CurrencyPair {
            0: CurrencySide::Quote,
            1: v,
        }
    }

    pub fn quote_max() -> Self {
        CurrencyPair {
            0: CurrencySide::Quote,
            1: Decimal::MAX,
        }
    }

    pub fn quote_zero() -> Self {
        CurrencyPair {
            0: CurrencySide::Quote,
            1: Decimal::ZERO,
        }
    }

    pub fn base_max() -> Self {
        CurrencyPair {
            0: CurrencySide::Base,
            1: Decimal::MAX,
        }
    }

    pub fn base_zero() -> Self {
        CurrencyPair {
            0: CurrencySide::Base,
            1: Decimal::ZERO,
        }
    }
}

// #[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSet<Value, Key = String>
where
    Key: Hash + Eq + Clone + Serialize, //  + for<'a> Deserialize<'a>,
    Value: UpdatedTrait + PacketTimeTrait + Serialize, //  + for<'a> Deserialize<'a>,
{
    ptime: PacketTime,
    utype: UpdateType,
    // #[serde_as(as = "Vec<(_, _)>")]
    datas: HashMap<Key, Arc<Value>>,
    cursor: CursorType,
}

impl<Value, Key> PacketTimeTrait for DataSet<Value, Key>
where
    Key: Hash + Eq + Clone + Serialize, //  + for<'a> Deserialize<'a>,
    Value: UpdatedTrait + PacketTimeTrait + Serialize, //  + for<'a> Deserialize<'a>,
{
    fn get_packet_time(&self) -> &PacketTime {
        &self.ptime
    }
}
impl<Value, Key> Default for DataSet<Value, Key>
where
    Key: Hash + Eq + Clone + Serialize, //  + for<'a> Deserialize<'a>,
    Value: UpdatedTrait + PacketTimeTrait + Serialize, //  + for<'a> Deserialize<'a>,
{
    fn default() -> Self {
        let initial_time = util::get_epoch_first();
        DataSet {
            utype: UpdateType::Snapshot,
            ptime: initial_time.into(),
            datas: HashMap::<Key, Arc<Value>>::default(),
            cursor: CursorType::None,
        }
    }
}

impl<Value, Key> DataSet<Value, Key>
where
    Key: Hash + Eq + Clone + Serialize, //  + for<'a> Deserialize<'a>,
    Value: UpdatedTrait + PacketTimeTrait + Serialize, //  + for<'a> Deserialize<'a>,
{
    pub fn get_datas(&self) -> &HashMap<Key, Arc<Value>> {
        &self.datas
    }

    pub fn get_datas_mut(&mut self) -> &mut HashMap<Key, Arc<Value>> {
        &mut self.datas
    }

    pub fn new(ptime: PacketTime, utype: UpdateType) -> Self {
        DataSet {
            utype: utype,
            ptime: ptime,
            datas: HashMap::<Key, Arc<Value>>::default(),
            cursor: CursorType::None,
        }
    }

    pub fn new_with_cursor(ptime: PacketTime, cursor: CursorType) -> Self {
        DataSet {
            utype: UpdateType::Partial,
            ptime: ptime,
            datas: HashMap::<Key, Arc<Value>>::default(),
            cursor: cursor,
        }
    }

    pub fn remove(&mut self, key: &Key) {
        self.ptime = Default::default();
        self.datas.remove(key);
    }

    pub fn insert_raw(&mut self, key: Key, value: Value) {
        self.insert(key, Arc::new(value));
    }

    pub fn insert(&mut self, key: Key, value: Arc<Value>) {
        if self.ptime.recvtime < value.get_packet_time().recvtime {
            self.ptime = value.get_packet_time().clone();
        }

        if let Some(ptr) = self.datas.get_mut(&key) {
            if ptr.get_updated() < value.get_updated() {
                *ptr = value;
            }
        } else {
            self.datas.insert(key, value);
        }
    }

    pub fn filter_new<P>(&self, mut predicate: P) -> (Self, HashMap<Key, Arc<Value>>)
    where
        Self: Sized,
        P: FnMut((&Key, &Arc<Value>)) -> bool,
    {
        let mut right = HashMap::<Key, Arc<Value>>::default();
        let mut wrong = HashMap::<Key, Arc<Value>>::default();
        for (key, value) in &self.datas {
            if predicate((key, value)) {
                right.insert(key.clone(), value.clone());
            } else {
                wrong.insert(key.clone(), value.clone());
            }
        }

        let os = DataSet {
            utype: self.utype.clone(),
            ptime: self.ptime.clone(),
            datas: right,
            cursor: CursorType::None,
        };
        (os, wrong)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Market {
    pub ptime: PacketTime,
    pub updated: chrono::DateTime<Utc>,

    pub kind: MarketKind,
    pub state: MarketState,

    pub quote_currency: String,
    pub base_currency: String,
    pub contract_size: Decimal,
    pub fee: FeeInfos,
    pub amount_limit: [CurrencyPair; 2],
    pub price_limit: [Decimal; 2],

    pub pp_kind: PrecisionKind,
    pub ap_kind: PrecisionKind,
    pub detail: serde_json::Value,
}
pub type MarketPtr = Arc<Market>;

impl Market {
    pub fn fix_price(
        &self,
        price: &Decimal,
        side: &OrderBookSide,
        lvl: i64,
    ) -> anyhow::Result<Decimal> {
        self.pp_kind.calculate_price(price, side, lvl)
    }

    pub fn fix_amount(
        &self,
        price: &Decimal,
        amount: &Decimal,
        lvl: i64,
    ) -> anyhow::Result<Decimal> {
        self.ap_kind.calculate_amount(price, amount, lvl)
    }
}

impl Hash for Market {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
    }
}

impl PartialEq for Market {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}

impl Eq for Market {}

impl UpdatedTrait for Market {
    fn get_updated(&self) -> &DateTime<Utc> {
        &self.updated
    }
}

impl PacketTimeTrait for Market {
    fn get_packet_time(&self) -> &PacketTime {
        &self.ptime
    }
}

#[derive(Default, Clone, Debug)]
pub enum MarketVal {
    #[default]
    None,
    Symbol(MarketKind),
    Pointer(MarketPtr),
}

impl MarketVal {
    pub fn symbol(&self) -> &str {
        match self {
            MarketVal::Symbol(s) => s.symbol(),
            MarketVal::Pointer(p) => p.kind.symbol(),
            MarketVal::None => "",
        }
    }

    pub fn has(&self) -> bool {
        match self {
            MarketVal::None => false,
            _ => true,
        }
    }

    pub fn is_ptr(&self) -> bool {
        match self {
            MarketVal::Pointer(_ptr) => true,
            _ => false,
        }
    }
    pub fn market_ptr(&self) -> Option<&MarketPtr> {
        match self {
            MarketVal::Pointer(ptr) => Some(ptr),
            _ => None,
        }
    }

    pub fn market_kind(&self) -> Option<&MarketKind> {
        match self {
            MarketVal::Pointer(ptr) => Some(&ptr.kind),
            MarketVal::Symbol(kind) => Some(kind),
            _ => None,
        }
    }
}

impl Serialize for MarketVal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            MarketVal::None => serializer.serialize_none(),
            MarketVal::Symbol(kind) => kind.serialize(serializer),
            MarketVal::Pointer(ptr) => ptr.kind.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for MarketVal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value: serde_json::Value = Deserialize::deserialize(deserializer)?;
        match value {
            serde_json::Value::Null => Ok(MarketVal::None),
            _ => {
                let kind = serde_json::from_value::<MarketKind>(value).map_err(|err| {
                    serde::de::Error::custom(format!("Failed to deserialize MarketKind: {}", err))
                })?;
                Ok(MarketVal::Symbol(kind))
            }
        }
    }
}

pub trait MarketTrait {
    fn get_market(&self) -> &MarketVal;
    fn get_market_kind(&self) -> Option<&MarketKind> {
        self.get_market().market_kind()
    }
    fn get_symbol(&self) -> &str {
        self.get_market().symbol()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataSetWithMarket<Value, Key = String>
where
    Key: Hash + Eq + Clone + Serialize,
    Value: UpdatedTrait + PacketTimeTrait + KeyTrait<Key> + Serialize,
{
    ptime: PacketTime,
    market: MarketVal,
    datas: HashMap<Key, Arc<Value>>,
    cursor: CursorType,
}

impl<Value, Key> MarketTrait for DataSetWithMarket<Value, Key>
where
    Key: Hash + Eq + Clone + Serialize,
    Value: UpdatedTrait + PacketTimeTrait + KeyTrait<Key> + Serialize,
{
    fn get_market(&self) -> &MarketVal {
        &self.market
    }
}

impl<Value, Key> PacketTimeTrait for DataSetWithMarket<Value, Key>
where
    Key: Hash + Eq + Clone + Serialize,
    Value: UpdatedTrait + PacketTimeTrait + KeyTrait<Key> + Serialize,
{
    fn get_packet_time(&self) -> &PacketTime {
        &self.ptime
    }
}

impl<Value, Key> DataSetWithMarket<Value, Key>
where
    Key: Hash + Eq + Clone + Serialize,
    Value: UpdatedTrait + PacketTimeTrait + KeyTrait<Key> + Serialize,
{
    pub fn new(ptime: PacketTime, market: MarketVal) -> Self {
        DataSetWithMarket {
            ptime: ptime,
            market: market,
            datas: HashMap::<Key, Arc<Value>>::default(),
            cursor: CursorType::None,
        }
    }

    pub fn new_with_cursor(ptime: PacketTime, market: MarketVal, cursor: CursorType) -> Self {
        DataSetWithMarket {
            ptime: ptime,
            market: market,
            datas: HashMap::<Key, Arc<Value>>::default(),
            cursor: cursor,
        }
    }

    pub fn remove(&mut self, key: &Key) {
        self.ptime = Default::default();
        self.datas.remove(key);
    }

    pub fn get_datas(&self) -> &HashMap<Key, Arc<Value>> {
        &self.datas
    }

    pub fn get_datas_mut(&mut self) -> &mut HashMap<Key, Arc<Value>> {
        &mut self.datas
    }

    pub fn len(&self) -> usize {
        self.datas.len()
    }

    pub fn filter_new<P>(&self, mut predicate: P) -> (Self, HashMap<Key, Arc<Value>>)
    where
        Self: Sized,
        P: FnMut((&Key, &Arc<Value>)) -> bool,
    {
        let mut right = HashMap::<Key, Arc<Value>>::default();
        let mut wrong = HashMap::<Key, Arc<Value>>::default();
        for (key, value) in &self.datas {
            if predicate((key, value)) {
                right.insert(key.clone(), value.clone());
            } else {
                wrong.insert(key.clone(), value.clone());
            }
        }

        let os = DataSetWithMarket {
            ptime: self.ptime.clone(),
            market: self.market.clone(),
            datas: right,
            cursor: CursorType::None,
        };
        (os, wrong)
    }

    pub fn get_market(&self) -> &MarketVal {
        &self.market
    }

    pub fn market_ptr(&self) -> Option<&MarketPtr> {
        self.market.market_ptr()
    }

    pub fn insert_raw(&mut self, value: Value) {
        self.insert(Arc::new(value));
    }

    pub fn insert(&mut self, value: Arc<Value>) {
        if self.ptime.recvtime < value.get_packet_time().recvtime {
            self.ptime = value.get_packet_time().clone();
        }

        if let Some(ptr) = self.datas.get_mut(value.get_key()) {
            if ptr.get_updated() < value.get_updated() {
                *ptr = value;
            }
        } else {
            self.datas.insert(value.get_key().clone(), value);
        }
    }

    pub fn insert_with_key(&mut self, key: Key, value: Arc<Value>) {
        if self.ptime.recvtime < value.get_packet_time().recvtime {
            self.ptime = value.get_packet_time().clone();
        }

        if let Some(ptr) = self.datas.get_mut(&key) {
            if ptr.get_updated() < value.get_updated() {
                *ptr = value;
            }
        } else {
            self.datas.insert(key, value);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    pub ptime: PacketTime,
    pub updated: chrono::DateTime<Utc>,

    pub currency: String,
    pub lock: Decimal,
    pub free: Decimal,

    pub detail: serde_json::Value,
}
pub type AssetPtr = Arc<Asset>;

impl UpdatedTrait for Asset {
    fn get_updated(&self) -> &DateTime<Utc> {
        &self.updated
    }
}

impl PacketTimeTrait for Asset {
    fn get_packet_time(&self) -> &PacketTime {
        &self.ptime
    }
}

impl Asset {
    pub fn total(&self) -> Decimal {
        self.lock + self.free
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub ptime: PacketTime,
    pub updated: chrono::DateTime<Utc>,

    pub side: OrderSide, // derive default를 사용하기 위하여. 만약 지정하지 않았으면 none이 맞긴하다(..)
    pub avg: Decimal,
    pub size: Decimal,
    pub unrealised_pnl: Decimal,
    pub leverage: Decimal,
    pub liquidation: Decimal,
    pub opened: chrono::DateTime<Utc>,

    pub detail: serde_json::Value,
}
pub type PositionPtr = Arc<Position>;
pub type PositionSet = DataSetWithMarket<Position, OrderSide>;

impl UpdatedTrait for Position {
    fn get_updated(&self) -> &DateTime<Utc> {
        &self.updated
    }
}

impl PacketTimeTrait for Position {
    fn get_packet_time(&self) -> &PacketTime {
        &self.ptime
    }
}

impl KeyTrait<OrderSide> for Position {
    fn get_key(&self) -> &OrderSide {
        &self.side
    }
}

pub type OrderBookQuote = (LazyDecimal, LazyDecimal);
pub type OrderBookQuotePtr = Arc<OrderBookQuote>;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBook {
    pub ptime: PacketTime,
    pub updated: chrono::DateTime<Utc>,
    pub market: MarketVal,

    pub ask: Vec<OrderBookQuotePtr>,
    pub bid: Vec<OrderBookQuotePtr>,

    pub detail: serde_json::Value,
}
pub type OrderBookPtr = Arc<OrderBook>;

impl UpdatedTrait for OrderBook {
    fn get_updated(&self) -> &DateTime<Utc> {
        &self.updated
    }
}

impl PacketTimeTrait for OrderBook {
    fn get_packet_time(&self) -> &PacketTime {
        &self.ptime
    }
}

impl MarketTrait for OrderBook {
    fn get_market(&self) -> &MarketVal {
        &self.market
    }
}

impl OrderBook {
    pub fn new(time: PacketTime, market: MarketVal, updated: DateTime<Utc>) -> Self {
        OrderBook {
            ptime: time,
            updated: updated,
            market: market,
            ask: Default::default(),
            bid: Default::default(),
            detail: serde_json::Value::default(),
        }
    }

    pub fn quotes(&self, side: &OrderBookSide) -> &Vec<OrderBookQuotePtr> {
        if *side == OrderBookSide::Bid {
            &self.bid
        } else {
            &self.ask
        }
    }

    pub async fn price(&self, side: &OrderBookSide, i: usize) -> anyhow::Result<Decimal> {
        let quotes = self.quotes(side);

        let (num, _) = quotes.get(i).ok_or(anyhowln!("invalid index"))?.as_ref();
        num.number().await
    }

    pub async fn amount(&self, side: &OrderBookSide, i: usize) -> anyhow::Result<Decimal> {
        let quotes = self.quotes(side);
        let (_, num) = quotes.get(i).ok_or(anyhowln!("invalid index"))?.as_ref();
        num.number().await
    }

    pub async fn numbers(
        &self,
        side: &OrderBookSide,
        i: usize,
    ) -> anyhow::Result<(Decimal, Decimal)> {
        let quotes = self.quotes(side);
        let (price_raw, amount_raw) = quotes.get(i).ok_or(anyhowln!("invalid index"))?.as_ref();
        let price = price_raw.number().await?;
        let amount = amount_raw.number().await?;
        Ok((price, amount))
    }

    pub async fn consume(
        &self,
        side: &OrderBookSide,
        mut remains: Vec<Decimal>,
        normalize: &str,
    ) -> anyhow::Result<Vec<(usize, Decimal)>> {
        let mut ret = Vec::<(usize, Decimal)>::default();
        let quotes = if *side == OrderBookSide::Ask {
            &self.ask
        } else {
            &self.bid
        };

        let mut acc = Decimal::ZERO;
        let mut total = Decimal::ZERO;
        for i in 0..quotes.len() {
            if remains.len() == 0 {
                break;
            }

            let (price_raw, amount_raw) = quotes[i].as_ref();
            let price = price_raw.number().await?;
            let mut amount = if normalize.is_empty() {
                amount_raw.number().await?
            } else {
                let n = meval::eval_str(normalize.replace("amount", amount_raw.string()))?;
                let amount = Decimal::from_f64(n).ok_or(anyhowln!(
                    "occur error(convert f64 to decimal) in orderbook:consume"
                ))?;
                amount
            };

            loop {
                if let Some(mut remain) =
                    remains.get(0).filter(|_v| amount != Decimal::ZERO).cloned()
                {
                    let min = remain.clone().min(amount.clone());
                    amount -= min;
                    remain -= min;
                    acc += price * min;
                    total += min;
                    if remain == Decimal::ZERO {
                        ret.push((i, acc / total));
                        remains.remove(0);
                        acc = Decimal::ZERO;
                        total = Decimal::ZERO;
                    } else {
                        remains[0] = remain;
                    }
                } else {
                    break;
                }
            }
        }

        Ok(ret)
    }

    pub async fn update_with_jarray(
        &mut self,
        side: OrderBookSide,
        items: serde_json::Value,
    ) -> anyhow::Result<()> {
        let quote = serde_json::from_value::<OrderBookQuote>(items)?;
        self.update_with_decimal(side, quote.into()).await
    }

    pub async fn update_with_json(
        &mut self,
        side: OrderBookSide,
        price: serde_json::Value,
        amount: serde_json::Value,
    ) -> anyhow::Result<()> {
        let p = serde_json::from_value::<LazyDecimal>(price)?;
        let a = serde_json::from_value::<LazyDecimal>(amount)?;
        self.update_with_decimal(side, (p, a).into()).await
    }

    pub async fn update_with_decimal(
        &mut self,
        side: OrderBookSide,
        newer: OrderBookQuotePtr,
    ) -> anyhow::Result<()> {
        let newer_price = newer.0.number().await?;
        let newer_amount = newer.1.number().await?;
        let quotes = if OrderBookSide::Ask == side {
            &mut self.ask
        } else {
            &mut self.bid
        };

        let pos = match side {
            OrderBookSide::Ask => {
                util::async_binary_search(quotes, |item| {
                    let cloned_item = item.clone();
                    let cloned_price = newer_price.clone();
                    async move {
                        let price = cloned_item.0.number().await.unwrap();
                        cloned_price.cmp(&price)
                    }
                    .boxed()
                })
                .await
            }
            OrderBookSide::Bid => {
                util::async_binary_search(quotes, |item| {
                    let cloned_item = item.clone();
                    let cloned_price = newer_price.clone();
                    async move {
                        let price = cloned_item.0.number().await.unwrap();
                        price.cmp(&cloned_price)
                    }
                    .boxed()
                })
                .await
            }
        };

        if newer_amount == Decimal::ZERO {
            let idx = pos.map_err(|_| anyhowln!("invalid price. it's not exist price"))?;
            quotes.remove(idx);
        } else {
            match pos {
                std::result::Result::Ok(idx) => {
                    if let Some(q) = quotes.get_mut(idx) {
                        *q = newer;
                    }
                }
                std::result::Result::Err(idx) => quotes.insert(idx, newer),
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicTrade {
    pub ptime: PacketTime,
    pub updated: chrono::DateTime<Utc>,

    // need changes : orderbook 처럼 string과 RwArc로 감싼 Decimal 객체를 별도로 만드는 게 좋다.
    pub quote: OrderBookQuote,
    pub side: OrderSide,

    pub detail: serde_json::Value,
}
pub type PublicTradePtr = Arc<PublicTrade>;

impl UpdatedTrait for PublicTrade {
    fn get_updated(&self) -> &DateTime<Utc> {
        &self.updated
    }
}

impl PacketTimeTrait for PublicTrade {
    fn get_packet_time(&self) -> &PacketTime {
        &self.ptime
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicTradeSet {
    ptime: PacketTime,
    datas: VecDeque<PublicTradePtr>,

    market: MarketVal,
    limit: usize,
}

impl Default for PublicTradeSet {
    fn default() -> Self {
        let initial_time = util::get_epoch_first();
        PublicTradeSet {
            ptime: initial_time.into(),
            datas: VecDeque::<PublicTradePtr>::default(),
            market: MarketVal::default(),
            limit: usize::MAX,
        }
    }
}

impl PublicTradeSet {
    pub fn get_datas(&self) -> &VecDeque<PublicTradePtr> {
        &self.datas
    }

    pub fn new(ptime: PacketTime, market: MarketVal, limit: Option<usize>) -> Self {
        PublicTradeSet {
            ptime: ptime,
            market: market,
            datas: VecDeque::<PublicTradePtr>::default(),
            limit: limit.unwrap_or(usize::MAX),
        }
    }

    pub fn insert_raw(&mut self, value: PublicTrade) -> Option<PublicTradePtr> {
        self.insert(Arc::new(value))
    }

    pub fn insert(&mut self, value: PublicTradePtr) -> Option<PublicTradePtr> {
        if self.ptime.recvtime < value.ptime.recvtime {
            self.ptime = value.ptime.clone();
        }

        if let Err(p) = self
            .datas
            .binary_search_by_key(value.get_updated(), |t| *t.get_updated())
        {
            self.datas.insert(p, value);
            if self.datas.len() > self.limit {
                return self.datas.pop_front();
            }
        }

        None
    }

    pub fn update(&mut self, other: Self) -> bool {
        if self.get_symbol() != other.get_symbol() {
            return false;
        }

        self.ptime = other.ptime;
        for trade in other.datas {
            self.insert(trade);
        }
        true
    }
}
impl MarketTrait for PublicTradeSet {
    fn get_market(&self) -> &MarketVal {
        &self.market
    }
}

impl PacketTimeTrait for PublicTradeSet {
    fn get_packet_time(&self) -> &PacketTime {
        &self.ptime
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderParam {
    pub kind: OrderKind,
    pub price: Decimal,
    pub amount: Decimal,
    pub side: OrderSide,

    pub is_postonly: bool,
    pub is_reduce: bool,
    pub cid: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AmendParam {
    P(Decimal),
    A(Decimal),
    PA((Decimal, Decimal)),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub ptime: PacketTime,
    pub updated: chrono::DateTime<Utc>,

    pub oid: String,
    pub cid: String, //

    pub kind: OrderKind,
    pub price: Decimal,
    pub amount: Decimal, //
    pub side: OrderSide,
    pub is_postonly: bool,
    pub is_reduce: bool,

    pub state: OrderState,
    pub avg: Decimal, //
    pub proceed: CurrencyPair,
    pub fee: CurrencyPair, //

    pub created: chrono::DateTime<Utc>, //

    pub detail: serde_json::Value, //
}
pub type OrderPtr = Arc<Order>;
pub type OrderSet = DataSetWithMarket<Order>;

impl UpdatedTrait for Order {
    fn get_updated(&self) -> &DateTime<Utc> {
        &self.updated
    }
}

impl PacketTimeTrait for Order {
    fn get_packet_time(&self) -> &PacketTime {
        &self.ptime
    }
}

impl KeyTrait<String> for Order {
    fn get_key(&self) -> &String {
        &self.oid
    }
}

impl Order {
    pub fn get_recved_duration(&self) -> chrono::Duration {
        Utc::now() - self.get_packet_time().recvtime
    }
    pub fn from_order_param(param: &OrderParam) -> Self {
        Order {
            ptime: PacketTime::default(),

            oid: String::default(),
            cid: param.cid.clone(),

            kind: param.kind.clone(),
            price: param.price.clone(),
            amount: param.amount.clone(),
            side: param.side.clone(),
            is_postonly: param.is_postonly.clone(),
            is_reduce: param.is_reduce.clone(),

            state: OrderState::default(),
            fee: CurrencyPair::default(),
            avg: Decimal::ZERO,
            proceed: CurrencyPair::default(),

            created: Utc::now(),
            updated: Utc::now(),
            detail: serde_json::Value::default(),
        }
    }

    pub fn remain(&self) -> Decimal {
        self.amount - self.proceed_real()
    }

    pub fn proceed_base(&self) -> Decimal {
        if self.avg == Decimal::ZERO {
            return Decimal::ZERO;
        }

        if self.proceed.0 == CurrencySide::Base {
            self.proceed.1.clone()
        } else {
            self.proceed.1 / self.avg
        }
    }

    pub fn proceed_real(&self) -> Decimal {
        self.proceed.1.clone()
    }

    pub fn proceed_quote(&self) -> Decimal {
        if self.proceed.0 == CurrencySide::Quote {
            self.proceed.1.clone()
        } else {
            self.proceed.1 * self.avg
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OrderResult {
    pub success: OrderSet,
    #[serde(
        serialize_with = "serialize_error_map",
        deserialize_with = "deserialize_error_map"
    )]
    pub errors: HashMap<String, anyhow::Error>,
}

impl OrderResult {
    pub fn new_with_orders(orders: OrderSet) -> Self {
        OrderResult {
            success: orders,
            errors: HashMap::<String, anyhow::Error>::default(),
        }
    }

    pub fn new(ptime: PacketTime, market: MarketVal) -> Self {
        let os = OrderSet::new(ptime, market);
        OrderResult {
            success: os,
            errors: HashMap::<String, anyhow::Error>::default(),
        }
    }
}

impl PacketTimeTrait for OrderResult {
    fn get_packet_time(&self) -> &PacketTime {
        &self.success.ptime
    }
}

impl MarketTrait for OrderResult {
    fn get_market(&self) -> &MarketVal {
        &self.success.market
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct OrdSerachParam {
    pub state: Option<OrderState>,
    pub page: Option<(String /*limit*/, String /*cur*/)>,
    pub opened: Option<chrono::DateTime<Utc>>,
    pub closed: Option<chrono::DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribeParam(pub serde_json::Value);

#[derive(Default)]
pub struct OrderbookSubscribeBuilder {
    market: Option<MarketPtr>,
    speed: SubscribeSpeed,
    quantity: SubscribeQuantity,
    additional: String,
}

impl OrderbookSubscribeBuilder {
    pub fn market(mut self, market: MarketPtr) -> Self {
        self.market = Some(market.clone());
        self
    }

    pub fn speed(mut self, speed: SubscribeSpeed) -> Self {
        self.speed = speed;
        self
    }

    pub fn quantity(mut self, quantity: SubscribeQuantity) -> Self {
        self.quantity = quantity;
        self
    }
    pub fn additional(mut self, additional: &str) -> Self {
        self.additional = additional.to_string();
        self
    }

    pub fn collect(self) -> SubscribeParam {
        let market = self.market.unwrap();
        let mut ret = json!({
            "market": serde_json::to_string(&market.kind).unwrap(),
            "symbol": market.kind.symbol(),
        });

        if self.speed != SubscribeSpeed::None {
            ret["speed"] = serde_json::Value::from(serde_json::to_string(&self.speed).unwrap());
        }
        if self.quantity != SubscribeQuantity::None {
            ret["quantity"] =
                serde_json::Value::from(serde_json::to_string(&self.quantity).unwrap());
        }
        if !self.additional.is_empty() {
            ret["additional"] = serde_json::Value::from(self.additional);
        }
        SubscribeParam(ret)
    }
}

#[derive(Default)]
pub struct MSASubscribeBuilder {
    market: Option<MarketPtr>,
    speed: SubscribeSpeed,
    additional: String,
}

impl MSASubscribeBuilder {
    pub fn market(mut self, market: MarketPtr) -> Self {
        self.market = Some(market.clone());
        self
    }

    pub fn speed(mut self, speed: SubscribeSpeed) -> Self {
        self.speed = speed;
        self
    }

    pub fn additional(mut self, additional: &str) -> Self {
        self.additional = additional.to_string();
        self
    }

    pub fn collect(self) -> SubscribeParam {
        let market = self.market.unwrap();
        let mut ret = json!({
            "market": serde_json::to_string(&market.kind).unwrap(),
            "symbol": market.kind.symbol(),
        });

        if self.speed != SubscribeSpeed::None {
            ret["speed"] = serde_json::Value::from(serde_json::to_string(&self.speed).unwrap());
        }
        if !self.additional.is_empty() {
            ret["additional"] = serde_json::Value::from(self.additional);
        }
        SubscribeParam(ret)
    }
}

#[derive(Default)]
pub struct MASubscribeBuilder {
    market: Option<MarketPtr>,
    additional: String,
}

impl MASubscribeBuilder {
    pub fn market(mut self, market: MarketPtr) -> Self {
        self.market = Some(market);
        self
    }

    pub fn additional(mut self, additional: String) -> Self {
        self.additional = additional;
        self
    }

    pub fn collect(self) -> SubscribeParam {
        let mut ret = serde_json::Value::default();
        if self.market.is_some() {
            let market = self.market.unwrap();
            ret["market"] = serde_json::Value::from(serde_json::to_string(&market.kind).unwrap());
            ret["symbol"] = serde_json::Value::from(market.kind.symbol());
        }

        if !self.additional.is_empty() {
            ret["additional"] = serde_json::Value::from(self.additional);
        }
        SubscribeParam(ret)
    }
}

#[repr(u32)]
#[derive(Eq, PartialEq, Clone, Debug, Hash, Serialize, Deserialize)]
pub enum SubscribeType {
    Orderbook = 1,
    PublicTrades = 2,
    Order = 3,
    Balance = 4,
    Position = 5,
}

impl SubscribeType {
    pub fn orderbook_param() -> OrderbookSubscribeBuilder {
        OrderbookSubscribeBuilder::default()
    }
    pub fn public_trades_param() -> MSASubscribeBuilder {
        MSASubscribeBuilder::default()
    }
    pub fn order_param() -> MASubscribeBuilder {
        MASubscribeBuilder::default()
    }
    pub fn balance_param() -> MASubscribeBuilder {
        MASubscribeBuilder::default()
    }
    pub fn position_param() -> MASubscribeBuilder {
        MASubscribeBuilder::default()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SubscribeResult {
    None,
    Authorized(bool),
    #[serde(
        serialize_with = "serialize_anyhow_error",
        deserialize_with = "deserialize_anyhow_error"
    )]
    Err(anyhow::Error),
    Orderbook(OrderBook),
    PublicTrades(HashMap<MarketKind, PublicTradeSet>),
    Order(HashMap<MarketKind, OrderSet>),
    Balance(DataSet<Asset>),

    Position(HashMap<MarketKind, PositionSet>),
}

impl From<anyhow::Error> for SubscribeResult {
    fn from(err: anyhow::Error) -> Self {
        SubscribeResult::Err(err)
    }
}

#[derive(Debug)]
pub struct ExchangeStorage {
    pub markets: RwArc<DataSet<Market, MarketKind>>,
    pub orderbook: RwArc<HashMap<MarketKind, Arc<OrderBook>>>,
    pub trades: RwArc<HashMap<MarketKind, PublicTradeSet>>,
    pub positions: RwArc<HashMap<MarketKind, PositionSet>>,
    pub orders: cache::LfuCache<String, (OrderPtr, MarketPtr)>,
    pub assets: RwArc<DataSet<Asset>>,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct RestAPIParam {
    pub url: String,
    pub proxy: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ExchangeKey {
    pub tag: String,
    pub exchange: String,
    pub is_testnet: bool,

    #[serde(deserialize_with = "cassry::util::deserialize_secret_string")]
    pub email: SecretString,

    #[serde(deserialize_with = "cassry::util::deserialize_secret_string")]
    pub key: SecretString,

    #[serde(deserialize_with = "cassry::util::deserialize_secret_string")]
    pub secret: SecretString,

    #[serde(deserialize_with = "cassry::util::deserialize_secret_string")]
    pub passphrase: SecretString,
    pub socks5ip: String,
    pub socks5port: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeConfig {
    #[serde(
        serialize_with = "serialize_chrono_duration",
        deserialize_with = "deserialize_chrono_duration"
    )]
    pub ping_interval: chrono::Duration,

    #[serde(
        serialize_with = "serialize_chrono_duration",
        deserialize_with = "deserialize_chrono_duration"
    )]
    pub eject: chrono::Duration,
    #[serde(
        serialize_with = "serialize_chrono_duration",
        deserialize_with = "deserialize_chrono_duration"
    )]
    pub sync_expired_duration: chrono::Duration, // order가 패킷으로 받더라도 후에 다시 갱신하기 위하여 expired가 있다
    #[serde(
        serialize_with = "serialize_chrono_duration",
        deserialize_with = "deserialize_chrono_duration"
    )]
    pub state_expired_duration: chrono::Duration, // ordering, cancelling 이후에 expired를 설정하는 시간

    pub opt_max_order_chche: usize,
    pub opt_max_trades_chche: usize,
}

impl Default for ExchangeConfig {
    fn default() -> Self {
        Self {
            ping_interval: chrono::Duration::minutes(1),
            eject: chrono::Duration::seconds(5),
            sync_expired_duration: chrono::Duration::minutes(5),
            state_expired_duration: chrono::Duration::minutes(1),

            opt_max_order_chche: 2000,
            opt_max_trades_chche: 2000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeParam {
    pub websocket: WebsocketParam,
    pub restapi: RestAPIParam,
    pub key: Arc<ExchangeKey>,
    pub config: ExchangeConfig,
    pub kind: MarketOpt,
}

pub struct ExchangeContext {
    pub param: ExchangeParam,
    pub storage: ExchangeStorage,
    pub requester: reqwest::Client,
    pub recorder: LocalDB,
}
pub type ExchangeContextPtr = Arc<ExchangeContext>;

impl ExchangeContext {
    pub fn new(param: ExchangeParam, recorder: LocalDB, requester: reqwest::Client) -> Self {
        let storage = ExchangeStorage {
            markets: RwLock::new(Default::default()).into(),
            orderbook: RwLock::new(Default::default()).into(),
            trades: RwLock::new(Default::default()).into(),
            positions: RwLock::new(Default::default()).into(),
            assets: RwLock::new(Default::default()).into(),
            orders: cache::LfuCache::<String, (OrderPtr, MarketPtr)>::new(
                param.config.opt_max_order_chche.clone(),
            ),
        };

        ExchangeContext {
            param: param,
            storage: storage,
            requester: requester,
            recorder: recorder,
        }
    }

    pub async fn find_market(&self, kind: &MarketKind) -> Option<MarketPtr> {
        let locked = self.storage.markets.read().await;
        locked.get_datas().get(kind).cloned()
    }

    pub async fn load_db_order(
        &self,
        oid: &str,
        _kind: &MarketKind,
    ) -> anyhow::Result<Option<Arc<(OrderPtr, MarketPtr)>>> {
        let str = self.recorder.get(oid.to_string()).await?;
        if str.is_none() {
            return Ok(None);
        }

        let mut value = serde_json::Value::from_str(&str.unwrap())?;
        let items = value.as_array_mut().ok_or(anyhowln!("invalid value"))?;

        let order = serde_json::from_value::<OrderPtr>(std::mem::replace(
            &mut items[0],
            serde_json::Value::default(),
        ))?;
        let kind = serde_json::from_value(std::mem::replace(
            &mut items[1],
            serde_json::Value::default(),
        ))?;
        let market = self
            .find_market(&kind)
            .await
            .ok_or(anyhowln!("invalid market"))?;
        let ret = Arc::new((order.clone(), market.clone()));
        self.cache_order(ret.clone()).await?;
        Ok(Some(ret))
    }

    pub async fn save_db_order(&self, order: OrderPtr, market: MarketPtr) -> anyhow::Result<()> {
        let order_val = serde_json::to_value(&order)?;
        let market_val = serde_json::to_value(&market.kind)?;
        let items = vec![order_val, market_val];
        let str = serde_json::to_string(&items)?;
        self.recorder.put(order.oid.clone(), str).await?;
        Ok(())
    }

    pub async fn cache_order(&self, order: Arc<(OrderPtr, MarketPtr)>) -> anyhow::Result<()> {
        let oid = order.0.oid.clone();
        if let Some(dump) = self.storage.orders.insert(oid, order).await {
            self.save_db_order(dump.0.clone(), dump.1.clone()).await?;
        }

        Ok(())
    }

    pub async fn find_order(
        &self,
        oid: &String,
        kind: &MarketKind,
    ) -> anyhow::Result<Option<OrderPtr>> {
        let item = if let Some((result, dump_opt)) = self.storage.orders.get(oid).await {
            if let Some(dump) = dump_opt {
                self.save_db_order(dump.0.clone(), dump.1.clone()).await?;
            }

            result
        } else {
            let opt = self.load_db_order(oid.as_str(), &kind).await?;
            if opt.is_none() {
                return Ok(None);
            }

            opt.unwrap()
        };

        if item.1.kind != *kind {
            return Ok(None);
        }

        let now = Utc::now();
        let order = item.0.clone();
        if order.state.synchronizable()
            && now > order.get_expired_date(&self.param.config.sync_expired_duration)
        {
            return Ok(None);
        }

        Ok(Some(order))
    }

    pub async fn update(&self, result: SubscribeResult) -> anyhow::Result<()> {
        match result {
            SubscribeResult::Orderbook(mut data) => {
                let market_kind = {
                    let kind = data.market.market_kind().ok_or(anyhowln!("invalid kind"))?;
                    let market = self
                        .find_market(kind)
                        .await
                        .ok_or(anyhowln!("cannot find market"))?;
                    data.market = MarketVal::Pointer(market);
                    data.market.market_kind().unwrap()
                };

                let mut locked = self.storage.orderbook.write().await;
                if let Some(ptr) = locked.get_mut(market_kind) {
                    *ptr = Arc::new(data);
                } else {
                    locked.insert(market_kind.clone(), Arc::new(data));
                }
            }
            SubscribeResult::PublicTrades(data) => {
                let mut locked = self.storage.trades.write().await;
                for (key, value) in data {
                    let set = if let Some(set) = locked.get_mut(&key) {
                        set
                    } else {
                        let market = self
                            .find_market(&key)
                            .await
                            .ok_or(anyhowln!("invalid market"))?;
                        if let Some(v) = locked.get_mut(&market.kind) {
                            v
                        } else {
                            locked.insert(
                                key,
                                PublicTradeSet::new(
                                    value.get_packet_time().clone(),
                                    MarketVal::Pointer(market.clone()),
                                    Some(self.param.config.opt_max_trades_chche),
                                ),
                            );
                            locked.get_mut(&market.kind).unwrap()
                        }
                    };

                    for v in value.get_datas() {
                        set.insert(v.clone());
                    }
                }
            }
            SubscribeResult::Order(data) => {
                for (key, value) in data {
                    let market = self
                        .find_market(&key)
                        .await
                        .ok_or(anyhowln!("invalid market"))?;

                    for (_oid, order) in value.get_datas() {
                        self.cache_order(Arc::new((order.clone(), market.clone())))
                            .await?;
                    }
                }
            }

            SubscribeResult::Balance(data) => {
                let mut locked = self.storage.assets.write().await;
                for (key, value) in data.get_datas() {
                    if value.total() == Decimal::ZERO {
                        locked.remove(key);
                    } else {
                        locked.insert(key.clone(), value.clone());
                    }
                }
            }
            SubscribeResult::Position(data) => {
                let mut locked = self.storage.positions.write().await;
                for (key, value) in data {
                    let set = if let Some(set) = locked.get_mut(&key) {
                        set
                    } else {
                        let market = self
                            .find_market(&key)
                            .await
                            .ok_or(anyhowln!("invalid market"))?;
                        if let Some(v) = locked.get_mut(&market.kind) {
                            v
                        } else {
                            locked.insert(
                                market.kind.clone(),
                                PositionSet::new(
                                    value.get_packet_time().clone(),
                                    MarketVal::Pointer(market.clone()),
                                ),
                            );
                            locked.get_mut(&market.kind).unwrap()
                        }
                    };

                    for (k, v) in value.get_datas() {
                        if v.size == Decimal::ZERO {
                            set.remove(k);
                        } else {
                            set.insert(v.clone());
                        }
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }
}
