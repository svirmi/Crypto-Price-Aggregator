// Aggregate top of book updates from a currency pair listed on https://www.bybit.com
// Documentation https://bybit-exchange.github.io/docs/

package exchange

import (
	"fmt"

	"github.com/hirokisan/bybit/v2"
	"github.com/johnwashburne/Crypto-Price-Aggregator/pkg/logger"
	"github.com/johnwashburne/Crypto-Price-Aggregator/pkg/symbol"
	"github.com/johnwashburne/Crypto-Price-Aggregator/pkg/ws"
)

type Bybit struct {
	updates chan MarketUpdate
	url     string
	name    string
	symbol  string
	valid   bool
	logger  *logger.Logger
}

// create new bybit.com struct
func NewBybit(pair symbol.CurrencyPair) *Bybit {
	c := make(chan MarketUpdate, updateBufSize)
	name := fmt.Sprintf("Bybit: %s", pair.Bybit)

	return &Bybit{
		updates: c,
		url:     "wss://stream-testnet.bybit.com/v5/public/linear",
		name:    name,
		symbol:  pair.Bybit,
		valid:   pair.Bybit != "",
		logger:  logger.Named(name),
	}
}

// Receive book data from bybit.com, send any top of book updates
// over the updates channel as a MarketUpdate struct
func (e *Bybit) Recv() {
	e.logger.Debug("connecting to socket Bybit")

	conn := ws.New(e.url)

	conn.SetOnConnect(func(c *ws.Client) error {
		// specify a function to run on websocket connection and reconnection

		pair := fmt.Sprintf("orderbook.1.%s", e.symbol)

		param := struct {
			Op   string        `json:"op"`
			Args []interface{} `json:"args"`
		}{
			Op:   "subscribe",
			Args: []interface{}{pair},
		}

		if err := c.WriteJSON(param); err != nil {
			return err
		}

		var resp bybit.V5WebsocketPublicOrderBookResponse

		if err := c.ReadJSON(&resp); err != nil {
			e.logger.Info(err)
			return err
		}
		return nil
	})

	if err := conn.Connect(); err != nil {
		e.logger.Warn("could not connect to socket, RETURNING")
		return
	}

	e.logger.Debug("connected to socket Bybit")

	for {

		var message bybit.V5WebsocketPublicOrderBookResponse

		if err := conn.ReadJSON(&message); err != nil {
			e.logger.Warn(err, " RETURNING")
			return
		}

		if message.Type == "delta" || message.Type == "snapshot" {

			ob := parseBybitBookData(&message)

			e.updates <- MarketUpdate{
				Bid:     ob.Bid,
				Ask:     ob.Ask,
				BidSize: ob.BidSize,
				AskSize: ob.AskSize,
				Name:    e.name,
			}
		}
	}

}

// parse a Bybit book websocket message into our market update object
// best bid and ask, as well as volume for both
func parseBybitBookData(c *bybit.V5WebsocketPublicOrderBookResponse) MarketUpdate {

	var ask string
	var askSize string

	if len(c.Data.Asks) == 1 {
		ask = c.Data.Asks[0].Price
		askSize = c.Data.Asks[0].Size
	}

	var bid string
	var bidSize string

	if len(c.Data.Bids) == 1 {
		bid = c.Data.Bids[0].Price
		bidSize = c.Data.Bids[0].Size
	}

	return MarketUpdate{
		Ask:     ask,
		AskSize: askSize,
		Bid:     bid,
		BidSize: bidSize,
	}
}

// Name of data source
func (e *Bybit) Name() string {
	return e.name
}

// Access to update channel
func (e *Bybit) Updates() chan MarketUpdate {
	return e.updates
}

func (e *Bybit) Valid() bool {
	return e.valid
}
