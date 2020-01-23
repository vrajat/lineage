package io.tokern.lineage.redshift.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

public class SqlProvider implements ArgumentsProvider {
  private static Logger logger = LoggerFactory.getLogger(SqlProvider.class);

  @Override
  public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) {
    Stream.Builder<Arguments> argumentsBuilder = Stream.builder();
    YAMLMapper mapper = new YAMLMapper();
    YAMLFactory factory = new YAMLFactory();
    for (String filename : extensionContext.getTags()) {
      try {
        JsonParser parser = factory.createParser(this.getClass().getResource(filename));
        List<TestCase> cases = mapper.readValue(parser, new TypeReference<List<TestCase>>(){});
        for(TestCase testCase : cases) {
          argumentsBuilder.add(testCase.getArgs());
        }
      } catch (IOException exc) {
        logger.warn("Failed to process " + filename + ":" + exc.getMessage());
      }
    }
    return argumentsBuilder.build();
  }
}
