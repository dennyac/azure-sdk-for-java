// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
// Code generated by Microsoft (R) AutoRest Code Generator.

package com.azure.resourcemanager.datafactory.generated;

import com.azure.core.util.BinaryData;
import com.azure.resourcemanager.datafactory.models.ActivityDependency;
import com.azure.resourcemanager.datafactory.models.ActivityOnInactiveMarkAs;
import com.azure.resourcemanager.datafactory.models.ActivityPolicy;
import com.azure.resourcemanager.datafactory.models.ActivityState;
import com.azure.resourcemanager.datafactory.models.BigDataPoolParametrizationReference;
import com.azure.resourcemanager.datafactory.models.BigDataPoolReferenceType;
import com.azure.resourcemanager.datafactory.models.ConfigurationType;
import com.azure.resourcemanager.datafactory.models.DependencyCondition;
import com.azure.resourcemanager.datafactory.models.LinkedServiceReference;
import com.azure.resourcemanager.datafactory.models.NotebookParameter;
import com.azure.resourcemanager.datafactory.models.NotebookParameterType;
import com.azure.resourcemanager.datafactory.models.NotebookReferenceType;
import com.azure.resourcemanager.datafactory.models.SparkConfigurationParametrizationReference;
import com.azure.resourcemanager.datafactory.models.SparkConfigurationReferenceType;
import com.azure.resourcemanager.datafactory.models.SynapseNotebookActivity;
import com.azure.resourcemanager.datafactory.models.SynapseNotebookReference;
import com.azure.resourcemanager.datafactory.models.UserProperty;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;

public final class SynapseNotebookActivityTests {
    @org.junit.jupiter.api.Test
    public void testDeserialize() throws Exception {
        SynapseNotebookActivity model = BinaryData.fromString(
            "{\"type\":\"SynapseNotebook\",\"typeProperties\":{\"notebook\":{\"type\":\"NotebookReference\",\"referenceName\":\"dataocepjsfhxhulrekr\"},\"sparkPool\":{\"type\":\"BigDataPoolReference\",\"referenceName\":\"datadnzrcjokgthy\"},\"parameters\":{\"vyceks\":{\"value\":\"datarwlguesoivaoryef\",\"type\":\"bool\"}},\"executorSize\":\"datat\",\"conf\":\"datagmfjzqv\",\"driverSize\":\"datadhixd\",\"numExecutors\":\"dataocsmcqs\",\"configurationType\":\"Customized\",\"targetSparkConfiguration\":{\"type\":\"SparkConfigurationReference\",\"referenceName\":\"dataa\"},\"sparkConfig\":{\"heoxs\":\"datazjdrkc\",\"tlxqhyy\":\"dataf\"}},\"linkedServiceName\":{\"referenceName\":\"zgxkwcqpv\",\"parameters\":{\"fylbokbriy\":\"datalkrroqsdvxdd\",\"buravswnnsb\":\"datarxae\",\"ojyn\":\"datakumxbcn\",\"fxiaptsukdoy\":\"datahbtycfj\"}},\"policy\":{\"timeout\":\"datazniekedxvw\",\"retry\":\"dataipxzzcxqd\",\"retryIntervalInSeconds\":2074445681,\"secureInput\":true,\"secureOutput\":true,\"\":{\"oxqwcusls\":\"datayb\",\"zwybbewjvyrd\":\"datatzq\",\"bwr\":\"dataw\"}},\"name\":\"bm\",\"description\":\"lmzaruosmpcajxua\",\"state\":\"Inactive\",\"onInactiveMarkAs\":\"Failed\",\"dependsOn\":[{\"activity\":\"lwrqhehnazck\",\"dependencyConditions\":[\"Succeeded\",\"Skipped\"],\"\":{\"oahektwgiumcco\":\"datazgi\",\"dhrkhfyaxi\":\"datajxxjaafr\"}},{\"activity\":\"cnzsimbgv\",\"dependencyConditions\":[\"Failed\",\"Succeeded\"],\"\":{\"wwyubkppocjyj\":\"dataqparbog\"}},{\"activity\":\"emgbkjxuxm\",\"dependencyConditions\":[\"Failed\"],\"\":{\"fekpgllezvrvjws\":\"datajfeanbn\",\"jbsvk\":\"datafkzlv\"}},{\"activity\":\"jynvguhqugnqs\",\"dependencyConditions\":[\"Skipped\",\"Succeeded\"],\"\":{\"xxsybtpqgxz\":\"datawhmncewcfinsoi\",\"sicnckdxflgj\":\"datagcl\",\"ilcerrpalxm\":\"databtce\",\"u\":\"datasbgj\"}}],\"userProperties\":[{\"name\":\"j\",\"value\":\"datanabyvmch\"},{\"name\":\"kwlmittpbivhkdxh\",\"value\":\"datavybxplbdaz\"}],\"\":{\"zu\":\"datagvd\"}}")
            .toObject(SynapseNotebookActivity.class);
        Assertions.assertEquals("bm", model.name());
        Assertions.assertEquals("lmzaruosmpcajxua", model.description());
        Assertions.assertEquals(ActivityState.INACTIVE, model.state());
        Assertions.assertEquals(ActivityOnInactiveMarkAs.FAILED, model.onInactiveMarkAs());
        Assertions.assertEquals("lwrqhehnazck", model.dependsOn().get(0).activity());
        Assertions.assertEquals(DependencyCondition.SUCCEEDED, model.dependsOn().get(0).dependencyConditions().get(0));
        Assertions.assertEquals("j", model.userProperties().get(0).name());
        Assertions.assertEquals("zgxkwcqpv", model.linkedServiceName().referenceName());
        Assertions.assertEquals(2074445681, model.policy().retryIntervalInSeconds());
        Assertions.assertEquals(true, model.policy().secureInput());
        Assertions.assertEquals(true, model.policy().secureOutput());
        Assertions.assertEquals(NotebookReferenceType.NOTEBOOK_REFERENCE, model.notebook().type());
        Assertions.assertEquals(BigDataPoolReferenceType.BIG_DATA_POOL_REFERENCE, model.sparkPool().type());
        Assertions.assertEquals(NotebookParameterType.BOOL, model.parameters().get("vyceks").type());
        Assertions.assertEquals(ConfigurationType.CUSTOMIZED, model.configurationType());
        Assertions.assertEquals(SparkConfigurationReferenceType.SPARK_CONFIGURATION_REFERENCE,
            model.targetSparkConfiguration().type());
    }

    @org.junit.jupiter.api.Test
    public void testSerialize() throws Exception {
        SynapseNotebookActivity model
            = new SynapseNotebookActivity().withName("bm").withDescription("lmzaruosmpcajxua")
                .withState(ActivityState.INACTIVE).withOnInactiveMarkAs(ActivityOnInactiveMarkAs.FAILED)
                .withDependsOn(Arrays.asList(
                    new ActivityDependency().withActivity("lwrqhehnazck")
                        .withDependencyConditions(
                            Arrays.asList(DependencyCondition.SUCCEEDED, DependencyCondition.SKIPPED))
                        .withAdditionalProperties(mapOf()),
                    new ActivityDependency().withActivity("cnzsimbgv")
                        .withDependencyConditions(
                            Arrays.asList(DependencyCondition.FAILED, DependencyCondition.SUCCEEDED))
                        .withAdditionalProperties(mapOf()),
                    new ActivityDependency().withActivity("emgbkjxuxm")
                        .withDependencyConditions(Arrays.asList(DependencyCondition.FAILED))
                        .withAdditionalProperties(mapOf()),
                    new ActivityDependency().withActivity("jynvguhqugnqs")
                        .withDependencyConditions(
                            Arrays.asList(DependencyCondition.SKIPPED, DependencyCondition.SUCCEEDED))
                        .withAdditionalProperties(mapOf())))
                .withUserProperties(Arrays.asList(new UserProperty().withName("j").withValue("datanabyvmch"),
                    new UserProperty().withName("kwlmittpbivhkdxh").withValue("datavybxplbdaz")))
                .withLinkedServiceName(new LinkedServiceReference().withReferenceName("zgxkwcqpv")
                    .withParameters(mapOf("fylbokbriy", "datalkrroqsdvxdd", "buravswnnsb", "datarxae", "ojyn",
                        "datakumxbcn", "fxiaptsukdoy", "datahbtycfj")))
                .withPolicy(new ActivityPolicy().withTimeout("datazniekedxvw").withRetry("dataipxzzcxqd")
                    .withRetryIntervalInSeconds(2074445681).withSecureInput(true).withSecureOutput(true)
                    .withAdditionalProperties(mapOf()))
                .withNotebook(new SynapseNotebookReference().withType(NotebookReferenceType.NOTEBOOK_REFERENCE)
                    .withReferenceName("dataocepjsfhxhulrekr"))
                .withSparkPool(new BigDataPoolParametrizationReference()
                    .withType(BigDataPoolReferenceType.BIG_DATA_POOL_REFERENCE).withReferenceName("datadnzrcjokgthy"))
                .withParameters(mapOf("vyceks",
                    new NotebookParameter().withValue("datarwlguesoivaoryef").withType(NotebookParameterType.BOOL)))
                .withExecutorSize("datat").withConf("datagmfjzqv").withDriverSize("datadhixd")
                .withNumExecutors("dataocsmcqs").withConfigurationType(ConfigurationType.CUSTOMIZED)
                .withTargetSparkConfiguration(new SparkConfigurationParametrizationReference()
                    .withType(SparkConfigurationReferenceType.SPARK_CONFIGURATION_REFERENCE).withReferenceName("dataa"))
                .withSparkConfig(mapOf("heoxs", "datazjdrkc", "tlxqhyy", "dataf"));
        model = BinaryData.fromObject(model).toObject(SynapseNotebookActivity.class);
        Assertions.assertEquals("bm", model.name());
        Assertions.assertEquals("lmzaruosmpcajxua", model.description());
        Assertions.assertEquals(ActivityState.INACTIVE, model.state());
        Assertions.assertEquals(ActivityOnInactiveMarkAs.FAILED, model.onInactiveMarkAs());
        Assertions.assertEquals("lwrqhehnazck", model.dependsOn().get(0).activity());
        Assertions.assertEquals(DependencyCondition.SUCCEEDED, model.dependsOn().get(0).dependencyConditions().get(0));
        Assertions.assertEquals("j", model.userProperties().get(0).name());
        Assertions.assertEquals("zgxkwcqpv", model.linkedServiceName().referenceName());
        Assertions.assertEquals(2074445681, model.policy().retryIntervalInSeconds());
        Assertions.assertEquals(true, model.policy().secureInput());
        Assertions.assertEquals(true, model.policy().secureOutput());
        Assertions.assertEquals(NotebookReferenceType.NOTEBOOK_REFERENCE, model.notebook().type());
        Assertions.assertEquals(BigDataPoolReferenceType.BIG_DATA_POOL_REFERENCE, model.sparkPool().type());
        Assertions.assertEquals(NotebookParameterType.BOOL, model.parameters().get("vyceks").type());
        Assertions.assertEquals(ConfigurationType.CUSTOMIZED, model.configurationType());
        Assertions.assertEquals(SparkConfigurationReferenceType.SPARK_CONFIGURATION_REFERENCE,
            model.targetSparkConfiguration().type());
    }

    // Use "Map.of" if available
    @SuppressWarnings("unchecked")
    private static <T> Map<String, T> mapOf(Object... inputs) {
        Map<String, T> map = new HashMap<>();
        for (int i = 0; i < inputs.length; i += 2) {
            String key = (String) inputs[i];
            T value = (T) inputs[i + 1];
            map.put(key, value);
        }
        return map;
    }
}
