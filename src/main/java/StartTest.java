import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.elasticsearch.action.search.SearchResponse;

import server.db.GameRepository;

public class StartTest {

	/**
	 * @param args
	 * @throws IOException
	 * @throws ElasticSearchException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException {

		System.out.println("Please wait for the initialization of the db.");
		Long startTime = System.currentTimeMillis();
		System.out.println("Start ElasticSearch...");
		GameRepository gameRepository = new GameRepository();
		System.out.println("Initialize data...");
		gameRepository.init();
		Long stopTime = System.currentTimeMillis();

		System.out.println("Game repository started in "
				+ (stopTime - startTime) / 1000 + " seconds.");
		System.out.println(gameRepository.getGameCount() + " games in db.");

		System.out
				.println("You can search after player names or for game details.");
		System.out.println("Type just the player name to search a player.");
		System.out
				.println("Type 'detail' followed by search string to search in the complete game.");
		System.out.println("Type 'shutdown' to stop the game repository.");

		handleConsoleInputs(gameRepository);
	}

	private static void handleConsoleInputs(GameRepository gameRepository) {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String line = null;
		try {
			while ((line = br.readLine()) != null && line.length() > 0) {
				if (line.equals("shutdown")) {
					GameRepository.shutDown();
					System.exit(0);
				} else if (line.startsWith("detail")) {
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
