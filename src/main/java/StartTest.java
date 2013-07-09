import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.search.SearchResponse;

import server.db.GameRepository;

public class StartTest {

	/**
	 * @param args
	 * @throws IOException
	 * @throws ElasticSearchException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws ElasticSearchException,
			IOException, InterruptedException {

		System.out.println("Please wait for the initialization of the db.");

		GameRepository gameRepository = new GameRepository();
		gameRepository.init();

		System.out.println(gameRepository.getGameCount() + " games in db.");

		System.out
				.println("You can search after player names or for game details. Type 'detail' followed by search string.");

		handleConsoleInputs(gameRepository);

		gameRepository.shutDown();
	}

	private static void handleConsoleInputs(GameRepository gameRepository) {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String line = null;
		try {
			while ((line = br.readLine()) != null && line.length() > 0) {
				if (line.startsWith("detail")) {
					searchForDetail(gameRepository, line.replace("detail ", ""));
				} else {
					searchForUser(gameRepository, line);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void searchForDetail(GameRepository gameRepository,
			String line) {
		SearchResponse searchForUser = gameRepository.searchForDetail(line);
		System.out.println(searchForUser.getHits().getTotalHits()
				+ " games found for string like " + line + ".");
		printStatisticsAndFirstHit(searchForUser);
	}

	private static void printStatisticsAndFirstHit(SearchResponse searchForUser) {
		System.out.println(searchForUser.getTook().getMillis() + " ms");
		if (searchForUser.getHits().getTotalHits() > 0) {
			System.out.println("First hit: "
					+ searchForUser.getHits().iterator().next()
							.sourceAsString());
		}
	}

	private static void searchForUser(GameRepository gameRepository, String line) {
		SearchResponse searchForUser = gameRepository.searchForUser(line);
		System.out.println(searchForUser.getHits().getTotalHits()
				+ " games found for " + line + " playing.");
		printStatisticsAndFirstHit(searchForUser);
	}
}
