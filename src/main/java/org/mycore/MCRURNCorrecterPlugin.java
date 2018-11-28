package org.mycore;

import java.util.ArrayList;
import java.util.Date;
import java.util.Optional;
import java.util.stream.Stream;

import javax.persistence.EntityManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mycore.access.MCRAccessException;
import org.mycore.backend.jpa.MCREntityManagerProvider;
import org.mycore.common.MCRException;
import org.mycore.common.MCRGsonUTCDateAdapter;
import org.mycore.datamodel.metadata.MCRBase;
import org.mycore.datamodel.metadata.MCRMetadataManager;
import org.mycore.datamodel.metadata.MCRObjectID;
import org.mycore.datamodel.metadata.MCRObjectService;
import org.mycore.frontend.cli.annotation.MCRCommand;
import org.mycore.frontend.cli.annotation.MCRCommandGroup;
import org.mycore.pi.MCRPIManager;
import org.mycore.pi.MCRPIService;
import org.mycore.pi.MCRPIServiceManager;
import org.mycore.pi.MCRPersistentIdentifier;
import org.mycore.pi.backend.MCRPI;
import org.mycore.pi.exceptions.MCRPersistentIdentifierException;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@MCRCommandGroup(name = "MyCoRe URN Correct Commands")
public class MCRURNCorrecterPlugin {

    private static final Logger LOGGER = LogManager.getLogger();

    @MCRCommand(
        syntax = "correct URN of {0}",
        help = "Corrects the URN of the object {0}")
    public static void correctURNofObject(String objectIDString)
        throws MCRPersistentIdentifierException, MCRAccessException {
        final MCRObjectID objectID = MCRObjectID.getInstance(objectIDString);
        if(!MCRMetadataManager.exists(objectID)){
            LOGGER.error("Object with id {} does not exist!", objectIDString);
            return;
        }
        final MCRBase mcrBase = MCRMetadataManager.retrieve(objectID);

        // correct identifier in metadata
        final MCRPIService<MCRPersistentIdentifier> service = MCRPIServiceManager.getInstance()
            .getRegistrationService("DNBURN");
        final Optional<MCRPersistentIdentifier> identifier = service.getMetadataService().getIdentifier(mcrBase, null);

        final String identifierAsString = identifier.map(MCRPersistentIdentifier::asString)
            .orElseThrow(() -> new MCRPersistentIdentifierException("Can not read identifier from " + objectIDString));

        final String cleanURNString = identifierAsString.replace("--", "-");
        final MCRPersistentIdentifier cleanURN = MCRPIManager.getInstance().getParserForType("dnbUrn")
            .parse(cleanURNString).get();

        service.getMetadataService().removeIdentifier(identifier.get(), mcrBase, null);
        service.getMetadataService().insertIdentifier(cleanURN, mcrBase, null);

        // correct identifier in object service flag
        MCRObjectService objectService = mcrBase.getService();

        ArrayList<String> flags = objectService.getFlags("MyCoRe-PI");
        Gson gson = getGson();
        String stringFlag = (String)flags.stream().filter((_stringFlag) -> {
            return gson.fromJson(_stringFlag, MCRPI.class).getService().equals("DNBURN");
        }).findAny()
            .orElseThrow(() -> new MCRException(new MCRPersistentIdentifierException("Could find flag to update!")));

        int flagIndex = objectService.getFlagIndex(stringFlag);
        objectService.removeFlag(flagIndex);

        final MCRPI newPIFlag = gson.fromJson(stringFlag, MCRPI.class);

        newPIFlag.setIdentifier(cleanURN.asString());
        MCRPIService.addFlagToObject(mcrBase, newPIFlag);

        // correct identifier in DB
        final MCRPI dnburn = MCRPIManager.getInstance().get("DNBURN", objectIDString, null);
        final EntityManager em = MCREntityManagerProvider.getCurrentEntityManager();
        MCRPIManager.getInstance()
            .delete(dnburn.getMycoreID(), dnburn.getAdditional(), dnburn.getType(), dnburn.getService());
        em.persist(new MCRPI(cleanURN.asString(), dnburn.getType(), dnburn.getMycoreID(), dnburn.getAdditional(), dnburn.getService(), dnburn.getRegistered()));

        MCRMetadataManager.update(mcrBase);
    }
    private static Gson getGson() {
        return (new GsonBuilder()).registerTypeAdapter(Date.class, new MCRGsonUTCDateAdapter()).setExclusionStrategies(new ExclusionStrategy[]{new ExclusionStrategy() {
            public boolean shouldSkipField(FieldAttributes fieldAttributes) {
                String name = fieldAttributes.getName();
                return Stream.of("mcrRevision", "mycoreID", "id", "mcrVersion").anyMatch((field) -> {
                    return field.equals(name);
                });
            }

            public boolean shouldSkipClass(Class<?> aClass) {
                return false;
            }
        }}).create();
    }
}
