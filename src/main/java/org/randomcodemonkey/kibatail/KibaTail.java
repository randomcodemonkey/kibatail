package org.randomcodemonkey.kibatail;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.utils.URIBuilder;
import org.randomcodemonkey.kibatail.client.KibaHttpClient;
import org.randomcodemonkey.kibatail.request.LogRequest.BooleanFilter;
import org.randomcodemonkey.kibatail.request.LogRequest.PhraseFilterMatch;
import org.randomcodemonkey.kibatail.response.LogPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class KibaTail implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(KibaTail.class);

  @Parameter(
      names = {"--debug", "-d"},
      description =
          "Enable debug logging. The LOG_LEVEL env variable can be used to directly set a"
              + " logging level (TRACE,DEBUG,INFO,WARN,ERROR). All internal logging of the"
              + " client is written to STDERR and the resulting log data from the remote "
              + " server is always written to STDOUT")
  private boolean debug = false;

  @Parameter(
      names = {"--help", "-h"},
      description = "Usage help",
      help = true)
  private boolean help;

  @Parameter(
      names = {"--index", "-i"},
      description = "Index pattern, for example 'application*'",
      required = true)
  private String index;

  @Parameter(
      names = {"--match", "-m"},
      description = "Add a log match pattern, for example 'app_name:hello-world'",
      required = false)
  private List<String> patterns = new ArrayList<>();

  @Parameter(
      names = {"--server", "-s"},
      description = "URL for kibana server, for example 'http://example.com:5601'",
      required = true)
  private String server;

  @Parameter(
      names = {"--user", "-u"},
      description = "Username to authenticate with",
      required = false)
  private String user;

  @Parameter(
      names = {"--password", "-p"},
      description = "Password for username",
      required = false)
  private String password;

  @Parameter(
      names = {"--fields", "-f"},
      description =
          "Comma-separated list of fields to print from each log row. Nested fields are supported and are"
              + " separated with '.', for example 'kubernetes.container.image'. The timestamp of the log row is"
              + " always included as the first column of the output. The available fields are environment and"
              + " service specific. The 'message' field is always available. Any field that is not available in"
              + " the log row is silently ignored. The special field '-' can be used to insert ' - ' to the output",
      required = false)
  private String fields =
      "log_level,service_name,thread_name,logger_name,-,message,stack_trace,kubernetes.container.name";

  private AtomicBoolean running = new AtomicBoolean();

  @Override
  public void run() {
    if (debug) {
      System.err.println("Enable debug logging");
      MDC.put("log-level", "DEBUG");
    }
    this.running.set(true);

    try {
      LOGGER.info("Create new client for server {}", server);
      KibaHttpClient client = new KibaHttpClient(new URIBuilder(server).build(), index, 60);

      if (user != null && password != null) {
        LOGGER.info("Authenticating as {}", user);
        client.setUser(user);
        client.setPassword(password);
      }

      for (String pattern : patterns) {
        String[] parts = pattern.split(":", 2);
        if (parts.length == 2) {
          BooleanFilter filter = new BooleanFilter();
          LOGGER.info("Add match pattern {} = {}", parts[0], parts[1]);
          filter.addShould(new PhraseFilterMatch().setPhrase(parts[0]).setValue(parts[1]));
          client.getRequest().addQueryFilter(filter);
        } else {
          throw new IllegalStateException(
              "Match pattern '" + pattern + "' is invalid, must be provided as 'field:pattern'");
        }
      }

      LogPrinter printer = new LogPrinter(fields);

      LOGGER.info("Start reading log data");
      while (this.running.get()) {
        try {
          printer.print(client.executeRequest());
        } catch (ClientProtocolException e) {
          LOGGER.warn("Request failure: {}", e.getMessage());
        } catch (IOException e) {
          LOGGER.warn("Request failure: {}", e.getMessage());
        } finally {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            throw new IllegalStateException("Interrupted while waiting to execute next request");
          }
        }
      }

    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid server URL: " + e.getMessage());
    }
  }

  public void shutdown() {
    this.running.set(false);
  }

  public static void main(String[] args) {
    try {
      KibaTail tail = new KibaTail();
      JCommander commander = JCommander.newBuilder().addObject(tail).build();
      commander.parse(args);
      if (tail.help) {
        commander.usage();
        System.exit(0);
      }
      tail.run();
    } catch (ParameterException e) {
      System.out.println(e.getMessage());
      System.out.println();
      System.out.println("Use --help for details");
      System.exit(1);
    } catch (Exception e) {
      LOGGER.error("KibaTail error: {}", e.getMessage(), e);
      System.exit(1);
    }
  }
}
