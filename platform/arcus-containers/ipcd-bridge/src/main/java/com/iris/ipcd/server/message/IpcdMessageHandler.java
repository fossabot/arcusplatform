/*
 * Copyright 2019 Arcus Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.iris.ipcd.server.message;

import java.io.StringReader;
import java.util.HashMap;

import com.iris.bridge.bus.PlatformBusService;
import com.iris.ipcd.session.IpcdClientToken;
import com.iris.messages.PlatformMessage;
import com.iris.messages.capability.PairingSubsystemCapability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.iris.bridge.bus.ProtocolBusService;
import com.iris.bridge.metrics.BridgeMetrics;
import com.iris.bridge.server.message.DeviceMessageHandler;
import com.iris.bridge.server.session.Session;
import com.iris.ipcd.delivery.IpcdDeliveryStrategy;
import com.iris.ipcd.delivery.IpcdDeliveryStrategyRegistry;
import com.iris.ipcd.server.session.IpcdSocketSession;
import com.iris.ipcd.session.IpcdSessionRegistry;
import com.iris.messages.address.Address;
import com.iris.population.PlacePopulationCacheManager;
import com.iris.protocol.ProtocolMessage;
import com.iris.protocol.ipcd.IpcdProtocol;
import com.iris.protocol.ipcd.message.IpcdMessage;
import com.iris.protocol.ipcd.message.model.Device;
import com.iris.protocol.ipcd.message.model.IpcdResponse;
import com.iris.protocol.ipcd.message.model.MessageType;

import com.iris.protocol.ipcd.message.serialize.IpcdSerDe;

import com.iris.util.IrisUUID;

@Singleton
public class IpcdMessageHandler implements DeviceMessageHandler<String> {
   private static final Logger logger = LoggerFactory.getLogger(IpcdMessageHandler.class);
   private final IpcdSerDe serializer = new IpcdSerDe();
   private final ProtocolBusService protocolBusService;
   private final PlatformBusService platformBusService;
   private final IpcdSessionRegistry sessionRegistry;
   private final IpcdDeliveryStrategyRegistry strategyRegistry;
   private final BridgeMetrics metrics;
   private final PlacePopulationCacheManager populationCacheMgr;

   @Inject
   public IpcdMessageHandler(ProtocolBusService protocolBusService,
      IpcdSessionRegistry sessionRegistry,
      IpcdDeliveryStrategyRegistry strategyRegistry,
      BridgeMetrics metrics,
      PlacePopulationCacheManager populationCacheMgr,
      PlatformBusService platformBusService
   ) {
      this.protocolBusService = protocolBusService;
      this.sessionRegistry = sessionRegistry;
      this.strategyRegistry = strategyRegistry;
      this.metrics = metrics;
      this.populationCacheMgr = populationCacheMgr;
      this.platformBusService = platformBusService;
   }

   @Override
   public String handleMessage(Session socketSession, String json) {
      try {
         logger.debug("Message from Device [{}]", json);
         logger.debug("With session [{}]", socketSession);
         IpcdMessage msg = serializer.parse(new StringReader(json));

         if (msg == null) throw new IllegalArgumentException("Invalid JSON for IPCD Message: [" + json + "]" );
         if (!msg.getMessageType().isClient()) throw new IllegalArgumentException("IPCD Message is illegal for client: " + json);
         IpcdSocketSession ipcdSession = (IpcdSocketSession)socketSession;

         if (!ipcdSession.isInitialized()) {
            initializeSession(ipcdSession, msg);
         }
         logger.debug("Check session [{}]", ipcdSession);
         if (isResponseHandledBySession(ipcdSession, msg)) {
            return null;
         }

         if (!IpcdClientToken.fromProtocolAddress(IpcdProtocol.ipcdAddress(msg.getDevice())).equals(ipcdSession.getClientToken())) {
            logger.warn("ALERT! potential device spoofing from [" + msg.getDevice() + "] against [" + ipcdSession.getClientToken() + "]");

            IpcdClientToken clientToken = IpcdClientToken.fromProtocolAddress(IpcdProtocol.ipcdAddress(msg.getDevice()));
            if (!ipcdSession.devices.contains(clientToken)) {
               ipcdSession.devices.add(clientToken);
               // ok, handle this as a hub.
               ipcdSession.addToSession(msg.getDevice());

               // Start pairing
               PlatformMessage startPairingMessage = PlatformMessage.builder()
                       .from(IpcdProtocol.ipcdAddress(msg.getDevice()))
                       .to(Address.platformService(ipcdSession.getActivePlace(), PairingSubsystemCapability.NAMESPACE))
                       .withPayload(PairingSubsystemCapability.StartPairingRequest.builder().withProductAddress("SERV:product:0c9a67").build())
                       .withPlaceId(ipcdSession.getActivePlace())
                       .withPopulation(populationCacheMgr.getPopulationByPlaceId(ipcdSession.getActivePlace()))
                       .withCorrelationId(IrisUUID.randomUUID().toString())
                       .isRequestMessage(true)
                       .create();
               platformBusService.placeMessageOnPlatformBus(startPairingMessage);

               HashMap<String, String> kv = new HashMap<String, String>();
               kv.put("IPCD:sn", msg.getDevice().getSn());
               kv.put("IPCD:v1devicetype", "InsteonSwitchLinc"); // XXX: hard coded

               // Now claim the device
               PlatformMessage platformMessage = PlatformMessage.builder()
                       .from(IpcdProtocol.ipcdAddress(msg.getDevice()))
                       .to(Address.platformService(ipcdSession.getActivePlace(), PairingSubsystemCapability.NAMESPACE))
                       .withPayload(PairingSubsystemCapability.SearchRequest.builder().withProductAddress("SERV:product:0c9a67").withForm(kv).build())
                       .withPlaceId(ipcdSession.getActivePlace())
                       .withPopulation(populationCacheMgr.getPopulationByPlaceId(ipcdSession.getActivePlace()))
                       .withCorrelationId(IrisUUID.randomUUID().toString())
                       .isRequestMessage(true)
                       .create();
               platformBusService.placeMessageOnPlatformBus(platformMessage);

               metrics.incProtocolMsgSentCounter();
               sessionRegistry.putSession(IpcdClientToken.fromProtocolAddress(IpcdProtocol.ipcdAddress(msg.getDevice())), socketSession);
            }
         }

         if(ipcdSession.getActivePlace() != null) {
            // Protocol Message Sent Here
            Device device = msg.getDevice();
            logger.debug("Sending message from [" + IpcdProtocol.ipcdAddress(device) + "] to message bus");
            ProtocolMessage protocolMessage = ProtocolMessage.builder()
                  .from(IpcdProtocol.ipcdAddress(device)) // XXX: does this mean there's device spoofing?
                  .to(Address.broadcastAddress())
                  .withPayload(IpcdProtocol.INSTANCE,msg)
                  .withPlaceId(ipcdSession.getActivePlace())
                  .withPopulation(populationCacheMgr.getPopulationByPlaceId(ipcdSession.getActivePlace()))
                  .create();
            protocolBusService.placeMessageOnProtocolBus(protocolMessage);
            metrics.incProtocolMsgSentCounter();

            IpcdDeliveryStrategy strategy = strategyRegistry.deliveryStrategyFor(socketSession.getClientToken(), msg.getDevice());
            strategy.onIpcdMessage(socketSession.getClientToken());
         }
      }
      catch (Exception ex) {
         logger.error("Exception while putting message on protocol bus.", ex);
      }
      return null;
   }

   private boolean isResponseHandledBySession(Session socketSession, IpcdMessage ipcdMessage) {
      if (ipcdMessage.getMessageType() == MessageType.response) {
         IpcdSocketSession ipcdSocketSession = (IpcdSocketSession)socketSession;
         IpcdResponse response = (IpcdResponse)ipcdMessage;
         String txnid = response.getRequest().getTxnid();
         if (ipcdSocketSession.hasTxnid(txnid)) {
            ipcdSocketSession.handleMessage(txnid, response);
            return true;
         }
      }
      return false;
   }

   private void initializeSession(IpcdSocketSession socketSession, IpcdMessage ipcdMessage) {
      socketSession.initializeSession(ipcdMessage.getDevice());
      sessionRegistry.putSession(socketSession);
   }
}

