package server.db;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;

public final class GameRepository {

	private final static String ISS = "iss";
	private final static String GAMES = "games";

	private final static Node node = nodeBuilder().clusterName("JSkat")
			.local(true).node();
	private final Client client;

	public GameRepository() {
		// on startup
		client = node.client();
		waitUntilIndexesAreInitialized();
		waitForGreenStatus();
	}

	private void waitForGreenStatus() {
		ClusterHealthRequestBuilder prepareHealth = client.admin().cluster()
				.prepareHealth(ISS);
		prepareHealth.setWaitForGreenStatus().execute().actionGet();
	}

	private void waitUntilIndexesAreInitialized() {
		ClusterHealthRequestBuilder prepareHealth = client.admin().cluster()
				.prepareHealth(ISS);
		prepareHealth.setWaitForActiveShards(1).execute().actionGet();
	}

	/**
	 * Imports the ISS data set if not available.
	 */
	public void init() {
		if (!issDataAvailable(ISS)) {
			System.out.println("No ISS data available, importing them now...");
			importData();
		}
	}

	private boolean issDataAvailable(String index) {
		IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest(
				index);
		ActionFuture<IndicesExistsResponse> exists = client.admin().indices()
				.exists(indicesExistsRequest);
		IndicesExistsResponse indicesExistsResponse = exists.actionGet();
		return indicesExistsResponse.isExists();
	}

	private void importData() {

		BufferedReader br = getFile();

		int lineCount = 0;
		String strLine;

		BulkRequestBuilder bulkRequest = createNewBulkRequest();

		try {
			while ((strLine = br.readLine()) != null) {

				XContentBuilder builder = createJson(strLine);

				bulkRequest.add(client.prepareIndex(ISS, GAMES).setSource(
						builder));

				lineCount++;

				if (lineCount % 10000 == 0) {
					executeBulkRequest(lineCount, bulkRequest);
					bulkRequest = createNewBulkRequest();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (bulkRequest.numberOfActions() > 0) {
			executeBulkRequest(lineCount, bulkRequest);
		}

		refreshIndex(ISS);
	}

	private BulkRequestBuilder createNewBulkRequest() {
		return client.prepareBulk();
	}

	private void executeBulkRequest(int gamesImportedSoFar,
			BulkRequestBuilder bulkRequest) {
		BulkResponse response = bulkRequest.execute().actionGet();
		evaluateSuccess(gamesImportedSoFar, response);
	}

	private void evaluateSuccess(int gamesImportedSoFar, BulkResponse response) {
		if (!response.hasFailures()) {
			System.out.println(gamesImportedSoFar + " games imported.");
		} else {
			System.out.println(gamesImportedSoFar
					+ " games imported. Some have failures.");
		}
	}

	private void refreshIndex(String index) {
		RefreshRequest request = new RefreshRequest(index);
		ActionFuture<RefreshResponse> refresh = client.admin().indices()
				.refresh(request);
		refresh.actionGet();
	}

	private XContentBuilder createJson(String strLine) throws IOException {
		XContentBuilder builder = jsonBuilder().startObject();

		parseLine(builder, strLine);

		builder.field("completegame", strLine);
		builder.endObject();
		return builder;
	}

	private BufferedReader getFile() {
		InputStream inputStream = this.getClass().getClassLoader()
				.getResourceAsStream("issgames.sgf");
		DataInputStream in = new DataInputStream(inputStream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		return br;
	}

	private void parseLine(XContentBuilder builder, String game)
			throws IOException {

		final Pattern summaryPartPattern = Pattern.compile("(\\w+)\\[(.*?)\\]"); //$NON-NLS-1$
		final Matcher summaryPartMatcher = summaryPartPattern.matcher(game);

		while (summaryPartMatcher.find()) {

			final String summaryPartMarker = summaryPartMatcher.group(1);
			final String summeryPart = summaryPartMatcher.group(2);

			parseSummaryPart(builder, summaryPartMarker, summeryPart);
		}
	}

	private static void parseSummaryPart(XContentBuilder builder,
			String summaryPartMarker, String summaryPart) throws IOException {

		if ("ID".equals(summaryPartMarker)) { //$NON-NLS-1$
			builder.field("gameID", summaryPart);
		} else if ("P0".equals(summaryPartMarker)) {
			builder.field("forehand", summaryPart);
		} else if ("P1".equals(summaryPartMarker)) {
			builder.field("middlehand", summaryPart);
		} else if ("P2".equals(summaryPartMarker)) {
			builder.field("rearhand", summaryPart);
		}
	}

	/**
	 * Gets the total count of games in database.
	 *
	 * @return Count of games
	 */
	public long getGameCount() {
		SearchResponse response = client.prepareSearch().execute().actionGet();
		return response.getHits().getTotalHits();
	}

	/**
	 * Shuts database down.
	 */
	public static void shutDown() {
		// on shutdown
		node.close();
	}

	/**
	 * Searches for games a user made on ISS.
	 *
	 * @param user
	 *            Login name of user
	 * @return Games the user took part
	 */
	public SearchResponse searchForUser(String user) {

		QueryBuilder qb = QueryBuilders.multiMatchQuery(user, "forehand",
				"middlehand", "rearhand");

		return executeQuery(qb);
	}

	/**
	 * Searches for a detail in the game summary (ISS format).
	 *
	 * @param detail
	 *            Search string
	 * @return Games where the detail is found
	 */
	public SearchResponse searchForDetail(String detail) {
		MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("completegame",
				detail);

		return executeQuery(matchQuery);
	}

	private SearchResponse executeQuery(QueryBuilder qb) {
		SearchResponse searchResponse = client.prepareSearch("iss")
				.setTypes("games").setQuery(qb).execute().actionGet();

		return searchResponse;
	}
}
