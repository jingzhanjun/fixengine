/*
 * Copyright (c) 2020 Georgios Voulgarakis
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.voulgarakis.spring.boot.starter.quickfixj.session;


import ch.voulgarakis.spring.boot.starter.quickfixj.authentication.AuthenticationService;
import ch.voulgarakis.spring.boot.starter.quickfixj.exception.QuickFixJConfigurationException;
import ch.voulgarakis.spring.boot.starter.quickfixj.exception.QuickFixJException;
import ch.voulgarakis.spring.boot.starter.quickfixj.exception.RejectException;
import ch.voulgarakis.spring.boot.starter.quickfixj.exception.SessionDroppedException;
import ch.voulgarakis.spring.boot.starter.quickfixj.flux.ReactiveFixSession;
import ch.voulgarakis.spring.boot.starter.quickfixj.session.logging.LoggingContext;
import ch.voulgarakis.spring.boot.starter.quickfixj.session.logging.LoggingId;
import ch.voulgarakis.spring.boot.starter.quickfixj.session.utils.FixMessageUtils;
import ch.voulgarakis.spring.boot.starter.quickfixj.session.utils.StartupLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import quickfix.Application;
import quickfix.Message;
import quickfix.RejectLogon;
import quickfix.SessionID;
import quickfix.field.Currency;
import quickfix.field.*;
import quickfix.fix50sp1.ExecutionReport;
import quickfix.fix50sp1.MarketDataIncrementalRefresh;
import quickfix.fix50sp1.MarketDataSnapshotFullRefresh;
import reactor.core.publisher.Flux;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

import static ch.voulgarakis.spring.boot.starter.quickfixj.session.utils.FixMessageUtils.isMessageOfType;

@Component
public class FixSessionManager implements Application {

    private static final Logger LOG = LoggerFactory.getLogger(FixSessionManager.class);
    private final Map<SessionID, ? extends AbstractFixSession> fixSessions;
    private final FixConnectionType fixConnectionType;
    private final StartupLatch startupLatch;
    private final LoggingId loggingId;
    private final AuthenticationService authenticationService;
    private static int accessNum=1;
    
    @Autowired 
    @Qualifier("TRADING")
    private ReactiveFixSession fixSession;
    @Autowired
    @Qualifier("TRADING_2")
    private ReactiveFixSession fixSession_2;
//    @Autowired private EsClient esClient;
//    @Autowired private DatabaseClient databaseClient;

    public FixSessionManager(Map<SessionID, ? extends AbstractFixSession> sessions,
                             FixConnectionType fixConnectionType,
                             StartupLatch startupLatch, LoggingId loggingId,
                             AuthenticationService authenticationService) {
        this.fixSessions = sessions;
        this.fixConnectionType = fixConnectionType;
        this.startupLatch = startupLatch;
        this.loggingId = loggingId;
        this.authenticationService = authenticationService;
    }

    private AbstractFixSession retrieveSession(SessionID sessionId) {
        AbstractFixSession fixSession = fixSessions.get(sessionId);
        if (Objects.isNull(fixSession)) {
            throw new QuickFixJConfigurationException(
                    String.format("No AbstractFixSession receiver for session [%s] ", sessionId));
        }
        return fixSession;
    }

    private Logger logger(SessionID sessionId) {
        if (Objects.isNull(sessionId)) {
            return LOG;
        }
        return Optional.ofNullable(fixSessions.get(sessionId))
                .map(AbstractFixSession::getClass)
                .map(LoggerFactory::getLogger)
                .orElse(LOG);
    }

    @Override
    public void onCreate(SessionID sessionId) {
        try (LoggingContext ignore = loggingId.loggingCtx(sessionId)) {
            logger(sessionId).info("Session created.");
            startupLatch.created(sessionId);
            retrieveSession(sessionId);
        }
    }

    @Override
    public void onLogon(SessionID sessionId) {
        try (LoggingContext ignore = loggingId.loggingCtx(sessionId)) {
            logger(sessionId).info("Session logged on.");
            startupLatch.loggedOn(sessionId);
            retrieveSession(sessionId).loggedOn();
        }
    }

    @Override
    public void onLogout(SessionID sessionId) {
        try (LoggingContext ignore = loggingId.loggingCtx(sessionId)) {
            if (fixConnectionType.isAcceptor()) {
                logger(sessionId).info("Session logged out.");
            } else {
                logger(sessionId).error("Session logged out.");
            }
            retrieveSession(sessionId).error(new SessionDroppedException());
        }
    }

    @Override
    public void toAdmin(Message message, SessionID sessionId) {
        try (LoggingContext ignore = loggingId.loggingCtx(sessionId)) {
            if (!isMessageOfType(message, MsgType.HEARTBEAT, MsgType.RESEND_REQUEST)) {
                if (isMessageOfType(message, MsgType.LOGON)) { // || isMessageOfType(message, MsgType.LOGOUT)) {
                    logger(sessionId).info("Sending login message: {}", message);
                    if (!fixConnectionType.isAcceptor()) {
                        authenticationService.authenticate(sessionId, message);
                    }
                } else {
                    LOG.debug("Sending administrative message: {}", message);
                }
                // retrieveSession(sessionId).sent(message);
            }
        } catch (RejectLogon rejectLogon) {
            logger(sessionId).error("Failed to authenticate message type: {}", message, rejectLogon);
            throw new QuickFixJException(rejectLogon);
        }
    }

    @Override
    public void fromAdmin(Message message, SessionID sessionId) throws RejectLogon {
        try (LoggingContext ignore = loggingId.loggingCtx(sessionId)) {
            //Heartbeat & Resend are omitted
            if (!isMessageOfType(message, MsgType.HEARTBEAT, MsgType.RESEND_REQUEST)) {
                logger(sessionId).debug("Received administrative message: {}", message);
                if (isMessageOfType(message, MsgType.LOGON)) {
                    AbstractFixSession fixSession = retrieveSession(sessionId);
                    if (fixConnectionType.isAcceptor()) {
                        authenticationService.authenticate(sessionId, message);
                    }
                    fixSession.loggedOn();
                } else if (isMessageOfType(message, MsgType.LOGOUT)) {
                    retrieveSession(sessionId).error(new SessionDroppedException(message));
                } else if (RejectException.isReject(message)) {
                    retrieveSession(sessionId).error(new RejectException(message));
                }
            }
        } catch (RejectLogon rejectLogon) {
            logger(sessionId).error("Failed to authenticate message type: {}", message,
                    rejectLogon);
            throw rejectLogon;
        } catch (Throwable e) {
            logger(sessionId).error("Failed to process FIX message: {}", message, e);
            throw e;
        }
    }

    @Override
    public void toApp(Message message, SessionID sessionId) {
        try (LoggingContext ignore = loggingId.loggingCtx(sessionId)) {
//            logger(sessionId).info("Sending message: {}", message);
            // retrieveSession(sessionId).sent(message);
        }
    }

    @Override
    public void fromApp(Message message, SessionID sessionId) {
        try (LoggingContext ignore = loggingId.loggingCtx(sessionId)) {
            if(FixMessageUtils.isMessageOfType(message, MsgType.MARKET_DATA_REQUEST)){
                if(accessNum==1){
                    LOG.info("Received a MarketDataRequest {}", message);
                    saveMarketDataRequest(message);
                    executePriceStream(message,sessionId);
                }
            }
            if(FixMessageUtils.isMessageOfType(message, MsgType.ORDER_SINGLE)){
                LOG.info("Received a NewOrderSingle {}", message);
                saveNewOrderSingle(message);
                executeNewOrderSingle(message);
            }
        } catch (Exception e) {
            logger(sessionId).error("Failed to process FIX message: {}", message, e);
            throw e;
        }finally {
            accessNum++;
        }
    }

    private void saveNewOrderSingle(Message message) {
        Map<String,Object> indexMap=new HashMap<>();
        indexMap.put("MsgType","D");
        String ClOrdID = FixMessageUtils.safeGetField(message, new ClOrdID()).orElse("no_request");
        indexMap.put("ClOrdID", ClOrdID);
        indexMap.put("Side",FixMessageUtils.safeGetField(message, new Side()).orElse('Z'));
        indexMap.put("TransactTime",FixMessageUtils.safeGetField(message, new TransactTime()).orElse(null));
        indexMap.put("OrdType",FixMessageUtils.safeGetField(message, new OrdType()).orElse('Z'));
//        esClient.addById(FIXConstants.TRADE_HUB_NEW_ORDER_SINGLE, ClOrdID, indexMap);
    }

    private void executeNewOrderSingle(Message message) {
        Message er=new ExecutionReport();
        //required
        er.setField(new ClOrdID(FixMessageUtils.safeGetField(message, new ClOrdID()).orElse("none")));
        er.setField(new OrderID(FixMessageUtils.safeGetField(message, new OrderID()).orElse("none")));
        er.setField(new ExecID("exec_accept_001"));
        er.setField(new ExecType(FixMessageUtils.safeGetField(message, new ExecType()).orElse('0')));
        er.setField(new OrdStatus(FixMessageUtils.safeGetField(message, new OrdStatus()).orElse('0')));
        er.setField(new Side(FixMessageUtils.safeGetField(message, new Side()).orElse('1')));
        er.setField(new LeavesQty(Double.valueOf("200.0")));
        er.setField(new CumQty(Double.valueOf("100.0")));
        //check type
        er.setField(new ExecRestatementReason(FixMessageUtils.safeGetField(message, new ExecRestatementReason()).orElse(1)));
        er.setField(new Price(Double.valueOf("20.02")));
        er.setField(new StopPx(Double.valueOf("25.02")));
        er.setField(new LastParPx(Double.valueOf("25.50")));
        er.setField(new ExpireDate("2021-09-06 12:12:12"));
        er.setField(new LastQty(Double.valueOf("200.00")));
        fixSession_2.send(()->er).subscribe();
    }
    
    private void saveMarketDataRequest(Message message) {
        //1.save in index
        String MsgType="V";
        String MDReqID = FixMessageUtils.safeGetField(message, new MDReqID()).orElse("no_request");
        Character SubcriptionRequestType = FixMessageUtils.safeGetField(message, new SubscriptionRequestType()).orElse('Z');
        Map<String,Object> indexMap=new HashMap<>();
        indexMap.put("MsgType",MsgType);
        indexMap.put("MDReqID", MDReqID);
        indexMap.put("SubscriptionRequestType", SubcriptionRequestType);
        //2.save in database
//        databaseClient.insert().into("marketdatarequest")
//                .value("MsgType",MsgType)
//                .value("MDReqID",MDReqID)
//                .value("SubcriptionRequestType",SubcriptionRequestType.toString())
//                .fetch().rowsUpdated()
//                .flatMap(i->{
//                    String result=esClient.addById(FIXConstants.PRICE_HUB_MARKET_DATA_REQUEST, MDReqID, indexMap);
//                    LOG.info("feign result:"+result);
//                    return Mono.just(1);
//                })
//                .onErrorResume(e -> {
//                    e = new ControllerException("保存请求价格流消息异常：" + e.getMessage());
//                    LOG.error(e.toString(), e);
//                    return Mono.just(0);
//                }).log().subscribe();

    }
    
    private void executePriceStream(Message message, SessionID sessionId) {
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
        String MDReqID = FixMessageUtils.safeGetField(message, new MDReqID()).orElse("no-quote-request-found");
        Character SubscriptionRequestType = FixMessageUtils.safeGetField(message, new SubscriptionRequestType()).orElse('2');
        try {
			if(SubscriptionRequestType.equals('0')){
			    String[] symbols=new String[]{"USD/CNY","GMD/CNY","GRD/CNY"};
			    List<MarketDataSnapshotFullRefresh> list=new ArrayList<>();
			    for(String s:symbols){
			        MarketDataSnapshotFullRefresh mdsf=new MarketDataSnapshotFullRefresh();
			        MarketDataSnapshotFullRefresh.NoMDEntries datas=new MarketDataSnapshotFullRefresh.NoMDEntries();
			        mdsf.setField(new MDReqID(MDReqID));
			        mdsf.setField(new Symbol(s));
			        List<Map<String,Object>> ml=new ArrayList<>();
			        for(int j=1;j<=5;j++){
			            Map<String,Object> priceMap=new HashMap<>();
			            priceMap.put("MDUpdateAction",'1');
			            priceMap.put("EntryType",'0');
			            priceMap.put("EntryPx",Double.valueOf(j+0.1));
			            priceMap.put("PriceType",Integer.valueOf(j));
			            priceMap.put("Currency","CNY");
			            ml.add(priceMap);
			        }
			        for (Map<String,Object> m : ml) {
			            datas.setField(new MDEntryType((char)m.get("EntryType")));
			            datas.setField(new MDEntryPx((Double)m.get("EntryPx")));
                        datas.setField(new PriceType((Integer) m.get("PriceType")));
			            datas.setField(new Currency((String) m.get("Currency")));
			            mdsf.addGroup(datas);
			        }
			        mdsf.setField(new MDEntryType('2'));
			        list.add(mdsf);
			    }
                Flux.interval(Duration.ofSeconds(1)).flatMap(e-> Flux.fromStream(list.stream())).flatMap(i->fixSession.send(()->i)).subscribe();
			}else if(SubscriptionRequestType.equals('1')){
			    String[] symbols=new String[]{"USD/CNY","GMD/CNY","GRD/CNY"};
			    List<MarketDataIncrementalRefresh> list=new ArrayList<>();
			    for(String s:symbols){
			        MarketDataIncrementalRefresh mdir=new MarketDataIncrementalRefresh();
			        MarketDataIncrementalRefresh.NoMDEntries datas=new MarketDataIncrementalRefresh.NoMDEntries();
			        mdir.setField(new MDReqID(MDReqID));
			        mdir.setField(new MDBookType(1));
			        mdir.setField(new TradeDate(sdf.format(new Date())));
			        List<Map<String,Object>> ml=new ArrayList<>();
			        for(int j=1;j<=5;j++){
			            Map<String,Object> priceMap=new HashMap<>();
                        priceMap.put("MDUpdateAction",'1');
			            priceMap.put("EntryType",'0');
			            priceMap.put("EntryPx",Double.valueOf(j+0.1));
			            priceMap.put("PriceType",Integer.valueOf(j));
			            priceMap.put("Currency","CNY");
			            ml.add(priceMap);
			        }
			        for (Map<String,Object> m : ml) {
                        datas.setField(new MDUpdateAction((char)m.get("MDUpdateAction")));
                        datas.setField(new MDEntryType((char)m.get("EntryType")));
                        datas.setField(new MDEntryPx((Double)m.get("EntryPx")));
                        datas.setField(new PriceType((Integer) m.get("PriceType")));
                        datas.setField(new Currency((String) m.get("Currency")));
                        mdir.addGroup(datas);
			        }
			        list.add(mdir);
			    }
                Flux.interval(Duration.ofSeconds(1)).flatMap(e-> Flux.fromStream(list.stream())).flatMap(i->fixSession.send(()->i)).subscribe();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}
