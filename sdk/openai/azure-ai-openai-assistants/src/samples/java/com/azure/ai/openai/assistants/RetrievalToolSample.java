// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.ai.openai.assistants;

import com.azure.ai.openai.assistants.models.Assistant;
import com.azure.ai.openai.assistants.models.AssistantCreationOptions;
import com.azure.ai.openai.assistants.models.AssistantThread;
import com.azure.ai.openai.assistants.models.AssistantThreadCreationOptions;
import com.azure.ai.openai.assistants.models.FileDetails;
import com.azure.ai.openai.assistants.models.FilePurpose;
import com.azure.ai.openai.assistants.models.MessageImageFileContent;
import com.azure.ai.openai.assistants.models.MessageRole;
import com.azure.ai.openai.assistants.models.MessageTextContent;
import com.azure.ai.openai.assistants.models.MessageTextDetails;
import com.azure.ai.openai.assistants.models.OpenAIFile;
import com.azure.ai.openai.assistants.models.OpenAIPageableListOfThreadMessage;
import com.azure.ai.openai.assistants.models.RetrievalToolDefinition;
import com.azure.ai.openai.assistants.models.RunStatus;
import com.azure.ai.openai.assistants.models.ThreadMessage;
import com.azure.ai.openai.assistants.models.ThreadRun;
import com.azure.core.credential.KeyCredential;
import com.azure.core.util.BinaryData;
import com.azure.core.util.Configuration;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Sample demonstrating the usage of one of the default tools: Retrieval.
 * This sample initially uploads a file to be used by the assistant. The file contains 2 arbitrary ids associated to fruits
 * After the initial setup of uploading the file, creating the assistant and the thread, instructing the assistant, we poll
 * the thread until we get a result. Running the sample should result on one of the ids return while the assistant requesting
 * for further information to be able to proceed.
 */
public class RetrievalToolSample {

    public static void main(String[] args) throws InterruptedException {
        String apiKey = Configuration.getGlobalConfiguration().get("NON_AZURE_OPENAI_KEY");
        String deploymentOrModelId = "gpt-4-1106-preview";
        String fileName = "retrieval_sample_java_sdk.txt";

        Path filePath = Paths.get("src", "samples", "resources", fileName);
        BinaryData fileData = BinaryData.fromFile(filePath);
        FileDetails fileDetails = new FileDetails(fileData).setFilename(fileName);

        AssistantsClient client = new AssistantsClientBuilder()
                .credential(new KeyCredential(apiKey))
                .buildClient();

        // Upload file for assistant
        OpenAIFile openAIFile = client.uploadFile(fileDetails, FilePurpose.ASSISTANTS);


        // Create assistant passing the file ID
        Assistant assistant = client.createAssistant(
            new AssistantCreationOptions(deploymentOrModelId)
                .setName("Java SDK Retrieval Sample")
                .setInstructions("You are a helpful assistant that can help fetch data from files you know about.")
                .setTools(Arrays.asList(new RetrievalToolDefinition()))
                .setFileIds(Arrays.asList(openAIFile.getId()))
        );

        // Create a thread with the assistant just created
        AssistantThread thread = client.createThread(new AssistantThreadCreationOptions());

        // Assign message to thread
        client.createMessage(
            thread.getId(),
            MessageRole.USER,
            "Can you give me the documented codes for 'banana' and 'orange'?");

        // Pass the message to the assistant and start the run
        ThreadRun run = client.createRun(thread, assistant);

        // Wait for the run to complete
        do {
            Thread.sleep(500);
            run = client.getRun(thread.getId(), run.getId());
        } while (run.getStatus() == RunStatus.IN_PROGRESS
            || run.getStatus() == RunStatus.QUEUED);

        // List messages from the thread
        OpenAIPageableListOfThreadMessage messages = client.listMessages(thread.getId());
        for (ThreadMessage message : messages.getData()) {
            message.getContent().forEach(content -> {
                if (content instanceof MessageTextContent) {
                    MessageTextDetails messageTextDetails = ((MessageTextContent) content).getText();
                    System.out.println(messageTextDetails.getValue());
                    messageTextDetails.getAnnotations().forEach(annotation ->
                        System.out.println("\tAnnotation start: " + annotation.getStartIndex()
                            + " ,end: " + annotation.getEndIndex() + " ,text: \"" + annotation.getText() + "\""));
                } else if (content instanceof MessageImageFileContent) {
                    System.out.print("Image file ID: ");
                    System.out.println(((MessageImageFileContent) content).getImageFile().getFileId());
                }
            });
        }
    }
}
