// PythonBridge.cs -- NinjaScript AddOn for bidirectional order bridge.
//
// Listens on TCP localhost:36973 (configurable in PORT below) for newline-
// delimited JSON commands from the Python auto-trader. Places orders via
// NinjaScript Account.CreateOrder / Submit, then pushes order/position
// events back over the same socket so Python knows about fills, rejects,
// and out-of-band state changes.
//
// Setup
// -----
// 1. Copy this file to:
//    Documents\NinjaTrader 8\bin\Custom\AddOns\PythonBridge.cs
//    (If Documents is OneDrive-redirected, use the actual resolved path.)
// 2. Open NT8 -> Tools -> NinjaScript Editor.
// 3. Right-click "AddOns" -> Compile (or restart NT -- it auto-compiles).
// 4. If you see "missing reference Newtonsoft.Json", right-click "AddOns" ->
//    References -> Add -> browse to:
//    C:\Program Files\NinjaTrader 8\bin\Newtonsoft.Json.dll
//    then recompile.
// 5. Verify success: open Tools -> NinjaScript Output. After NT restart you
//    should see "[PythonBridge] Listening on 127.0.0.1:36973".
//
// Protocol
// --------
// Commands from Python (one JSON per line):
//   {"cmd":"PING"}
//   {"cmd":"PLACE","id":"<py_id>","account":"Sim101","instrument":"MGC 06-26",
//    "action":"BUY","qty":2,"orderType":"MARKET","tif":"DAY"}
//
// Events from AddOn (one JSON per line):
//   {"event":"PONG"}
//   {"event":"ACK","id":"<py_id>","ntOrderId":"<NT_ORDER_ID>"}
//   {"event":"REJECT","id":"<py_id>","reason":"..."}
//   {"event":"ORDER_UPDATE","id":"<py_id>","ntOrderId":"...",
//    "state":"Working|Filled|Cancelled|Rejected","filledQty":N,
//    "avgFillPrice":X.X,"reason":"..."}
//   {"event":"POSITION","account":"...","instrument":"...","qty":N,
//    "avgPrice":X.X,"marketPosition":"Long|Short|Flat"}
//   {"event":"ERROR","msg":"..."}
//
// Single-client model: only one Python connection at a time. A new
// connection drops the old one. This matches the auto-trader pattern
// (one webhook server connecting to one NT instance).

#region Using declarations
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using NinjaTrader.Cbi;
using NinjaTrader.Core;
using NinjaTrader.NinjaScript;
using NinjaTrader.NinjaScript.AddOns;
using Newtonsoft.Json;
#endregion

namespace NinjaTrader.NinjaScript.AddOns
{
    public class PythonBridge : AddOnBase
    {
        // Change PORT here if 36973 is already in use on your machine.
        private const int PORT = 36973;
        private const string ADDON_NAME = "PythonBridge";

        private TcpListener _listener;
        private Thread _listenerThread;
        private TcpClient _client;
        private NetworkStream _stream;
        private readonly object _writeLock = new object();
        private volatile bool _running = false;

        private readonly object _subscribeLock = new object();
        private readonly HashSet<string> _subscribedAccounts = new HashSet<string>();

        // Map NT order Id -> Python correlation id (so OrderUpdate events can
        // find the original PLACE command).
        private readonly ConcurrentDictionary<string, string> _ntOrderToPyId =
            new ConcurrentDictionary<string, string>();

        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Name = ADDON_NAME;
                Description = "TCP bridge for Python auto-trader on port " + PORT;
            }
            else if (State == State.Configure)
            {
                StartServer();
            }
            else if (State == State.Terminated)
            {
                StopServer();
            }
        }

        // ===================================================================
        // Server lifecycle
        // ===================================================================

        private void StartServer()
        {
            try
            {
                _listener = new TcpListener(IPAddress.Loopback, PORT);
                _listener.Start();
                _running = true;
                _listenerThread = new Thread(AcceptLoop)
                {
                    IsBackground = true,
                    Name = ADDON_NAME + "-Listener",
                };
                _listenerThread.Start();
                Log("Listening on 127.0.0.1:" + PORT);
            }
            catch (Exception ex)
            {
                Log("Failed to start listener: " + ex.Message);
            }
        }

        private void StopServer()
        {
            _running = false;

            try { _listener?.Stop(); } catch { }
            try { _client?.Close(); } catch { }

            // Unsubscribe from accounts so we don't leak handlers across NT restart
            lock (_subscribeLock)
            {
                foreach (var acctName in _subscribedAccounts)
                {
                    var acct = Account.All.FirstOrDefault(a => a.Name == acctName);
                    if (acct != null)
                    {
                        try { acct.OrderUpdate -= OnOrderUpdate; } catch { }
                        try { acct.PositionUpdate -= OnPositionUpdate; } catch { }
                    }
                }
                _subscribedAccounts.Clear();
            }
            Log("Stopped");
        }

        private void AcceptLoop()
        {
            while (_running)
            {
                TcpClient client;
                try
                {
                    client = _listener.AcceptTcpClient();
                }
                catch (Exception ex)
                {
                    if (_running) Log("Accept error: " + ex.Message);
                    return;
                }
                HandleClient(client);
            }
        }

        private void HandleClient(TcpClient client)
        {
            // Single-client model: drop any existing client.
            try { _client?.Close(); } catch { }

            _client = client;
            _stream = client.GetStream();
            try
            {
                Log("Client connected from " + client.Client.RemoteEndPoint);
            }
            catch { Log("Client connected"); }

            try
            {
                using (var reader = new StreamReader(
                    _stream, Encoding.UTF8, false, 4096, leaveOpen: true))
                {
                    string line;
                    while (_running && (line = reader.ReadLine()) != null)
                    {
                        try
                        {
                            HandleCommand(line);
                        }
                        catch (Exception ex)
                        {
                            Log("Command error: " + ex.Message);
                            SendEvent(new Dictionary<string, object> {
                                {"event", "ERROR"}, {"msg", ex.Message},
                            });
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log("Client loop ended: " + ex.Message);
            }
            finally
            {
                Log("Client disconnected");
                try { client.Close(); } catch { }
                _client = null;
                _stream = null;
            }
        }

        // ===================================================================
        // Command handling
        // ===================================================================

        private void HandleCommand(string line)
        {
            if (string.IsNullOrWhiteSpace(line)) return;

            Dictionary<string, object> msg;
            try
            {
                msg = JsonConvert.DeserializeObject<Dictionary<string, object>>(line);
            }
            catch (Exception ex)
            {
                SendEvent(new Dictionary<string, object> {
                    {"event", "ERROR"}, {"msg", "Bad JSON: " + ex.Message},
                });
                return;
            }
            if (msg == null) return;

            object cmdObj;
            if (!msg.TryGetValue("cmd", out cmdObj)) return;
            var cmd = cmdObj == null ? "" : cmdObj.ToString();

            switch (cmd)
            {
                case "PING":
                    SendEvent(new Dictionary<string, object> { {"event", "PONG"} });
                    break;

                case "PLACE":
                    HandlePlace(msg);
                    break;

                default:
                    SendEvent(new Dictionary<string, object> {
                        {"event", "ERROR"}, {"msg", "Unknown cmd: " + cmd},
                    });
                    break;
            }
        }

        private void HandlePlace(Dictionary<string, object> msg)
        {
            string pyId = GetString(msg, "id");
            string accountName = GetString(msg, "account");
            string instrumentStr = GetString(msg, "instrument");
            string action = GetString(msg, "action");
            int qty = (int)GetLong(msg, "qty");
            string tif = GetString(msg, "tif", "DAY");

            if (string.IsNullOrEmpty(pyId))
            {
                SendEvent(new Dictionary<string, object> {
                    {"event", "ERROR"}, {"msg", "PLACE missing id"},
                });
                return;
            }

            if (qty <= 0)
            {
                SendReject(pyId, "Invalid qty: " + qty);
                return;
            }

            // Resolve account
            Account account = Account.All.FirstOrDefault(a => a.Name == accountName);
            if (account == null)
            {
                SendReject(pyId, "Account not found: " + accountName);
                return;
            }

            // Resolve instrument (e.g., "MGC 06-26"). NT does a lookup against
            // the configured instrument list; the connection must be active
            // and the symbol must match exactly.
            Instrument instrument;
            try
            {
                instrument = Instrument.GetInstrument(instrumentStr);
            }
            catch (Exception ex)
            {
                SendReject(pyId, "GetInstrument threw: " + ex.Message);
                return;
            }
            if (instrument == null)
            {
                SendReject(pyId, "Instrument not found: " + instrumentStr);
                return;
            }

            EnsureSubscribed(account);

            OrderAction oa;
            switch ((action ?? "").ToUpperInvariant())
            {
                case "BUY":  oa = OrderAction.Buy;  break;
                case "SELL": oa = OrderAction.Sell; break;
                default:
                    SendReject(pyId, "Invalid action: " + action);
                    return;
            }

            TimeInForce tf = ((tif ?? "").ToUpperInvariant() == "GTC")
                ? TimeInForce.Gtc : TimeInForce.Day;

            Order order;
            try
            {
                order = account.CreateOrder(
                    instrument: instrument,
                    orderAction: oa,
                    orderType: OrderType.Market,
                    orderEntry: OrderEntry.Automated,
                    timeInForce: tf,
                    quantity: qty,
                    limitPrice: 0,
                    stopPrice: 0,
                    oco: "",
                    name: "py:" + pyId,
                    gtd: Globals.MaxDate
                );
            }
            catch (Exception ex)
            {
                SendReject(pyId, "CreateOrder failed: " + ex.Message);
                return;
            }

            // Track BEFORE submit -- some events fire synchronously on Submit().
            _ntOrderToPyId[order.Id] = pyId;

            try
            {
                account.Submit(new[] { order });
            }
            catch (Exception ex)
            {
                string discard;
                _ntOrderToPyId.TryRemove(order.Id, out discard);
                SendReject(pyId, "Submit failed: " + ex.Message);
                return;
            }

            SendEvent(new Dictionary<string, object> {
                {"event", "ACK"},
                {"id", pyId},
                {"ntOrderId", order.Id},
            });
        }

        // ===================================================================
        // Account event subscription
        // ===================================================================

        private void EnsureSubscribed(Account account)
        {
            lock (_subscribeLock)
            {
                if (_subscribedAccounts.Contains(account.Name)) return;
                _subscribedAccounts.Add(account.Name);
                account.OrderUpdate += OnOrderUpdate;
                account.PositionUpdate += OnPositionUpdate;
                Log("Subscribed to events on account: " + account.Name);
            }
        }

        private void OnOrderUpdate(object sender, OrderEventArgs e)
        {
            string pyId;
            if (!_ntOrderToPyId.TryGetValue(e.Order.Id, out pyId))
            {
                return; // Not our order -- ignore (manual NT trade or stale)
            }

            var evt = new Dictionary<string, object> {
                {"event", "ORDER_UPDATE"},
                {"id", pyId},
                {"ntOrderId", e.Order.Id},
                {"state", e.Order.OrderState.ToString()},
                {"filledQty", e.Order.Filled},
                {"avgFillPrice", e.Order.AverageFillPrice},
                {"reason", e.Order.Error == ErrorCode.NoError ? "" : e.Order.Error.ToString()},
                {"nativeError", e.Order.NativeError ?? ""},
            };
            SendEvent(evt);

            // Drop completed orders so the map doesn't grow without bound.
            bool done =
                e.Order.OrderState == OrderState.Filled ||
                e.Order.OrderState == OrderState.Cancelled ||
                e.Order.OrderState == OrderState.Rejected;
            if (done)
            {
                string discard;
                _ntOrderToPyId.TryRemove(e.Order.Id, out discard);
            }
        }

        private void OnPositionUpdate(object sender, PositionEventArgs e)
        {
            int qty = e.Position.Quantity;
            if (e.Position.MarketPosition == MarketPosition.Short) qty = -qty;
            else if (e.Position.MarketPosition == MarketPosition.Flat) qty = 0;

            SendEvent(new Dictionary<string, object> {
                {"event", "POSITION"},
                {"account", e.Position.Account.Name},
                {"instrument", e.Position.Instrument.FullName},
                {"qty", qty},
                {"avgPrice", e.Position.AveragePrice},
                {"marketPosition", e.Position.MarketPosition.ToString()},
            });
        }

        // ===================================================================
        // Outbound write
        // ===================================================================

        private void SendReject(string pyId, string reason)
        {
            SendEvent(new Dictionary<string, object> {
                {"event", "REJECT"},
                {"id", pyId},
                {"reason", reason},
            });
        }

        private void SendEvent(Dictionary<string, object> evt)
        {
            string json;
            try
            {
                json = JsonConvert.SerializeObject(evt);
            }
            catch (Exception ex)
            {
                Log("Serialize failed: " + ex.Message);
                return;
            }
            byte[] bytes = Encoding.UTF8.GetBytes(json + "\n");

            lock (_writeLock)
            {
                if (_stream == null) return;
                try
                {
                    _stream.Write(bytes, 0, bytes.Length);
                    _stream.Flush();
                }
                catch (Exception ex)
                {
                    Log("Write failed (client likely dropped): " + ex.Message);
                    try { _stream.Close(); } catch { }
                    _stream = null;
                }
            }
        }

        // ===================================================================
        // Helpers
        // ===================================================================

        private static string GetString(Dictionary<string, object> d, string key, string def = "")
        {
            object v;
            if (d != null && d.TryGetValue(key, out v) && v != null) return v.ToString();
            return def;
        }

        private static long GetLong(Dictionary<string, object> d, string key, long def = 0)
        {
            object v;
            if (d == null || !d.TryGetValue(key, out v) || v == null) return def;
            long n;
            if (v is long) return (long)v;
            if (v is int) return (int)v;
            if (long.TryParse(v.ToString(), out n)) return n;
            return def;
        }

        private void Log(string msg)
        {
            // Writes to NinjaScript Output window (Tools -> NinjaScript Output).
            NinjaTrader.Code.Output.Process(
                "[" + ADDON_NAME + "] " + msg,
                PrintTo.OutputTab1);
        }
    }
}
