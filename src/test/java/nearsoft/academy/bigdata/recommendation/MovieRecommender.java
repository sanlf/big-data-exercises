package nearsoft.academy.bigdata.recommendation;

import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class MovieRecommender{
    private String moviesPath;
    private int totalReviews;
    private int totalProducts;
    private int totalUsers;
    private BiMap<String, Integer> productHash;
    private BiMap<String, Integer> usersHash;

    private final int NUMBER_RECOMMENDATIONS;
    private final String OUTPUT_FILENAME;


    public MovieRecommender(String moviesPath) throws IOException {
        this.NUMBER_RECOMMENDATIONS = 3;
        this.OUTPUT_FILENAME = "recommendations.csv";

        this.moviesPath = moviesPath;
        this.totalProducts = 0;
        this.totalReviews = 0;
        this.totalUsers = 0;
        this.productHash = HashBiMap.create();
        this.usersHash = HashBiMap.create();

        this.readWriteMovies(moviesPath);
    }

    public List<String> getRecommendationsForUser(String user) throws IOException, TasteException {

        // lines taken from http://mahout.apache.org/users/recommender/userbased-5-minutes.html
        DataModel model = new FileDataModel(new File(this.OUTPUT_FILENAME));
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
        UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model);
        UserBasedRecommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);

        // gets NUMBER_RECOMMENDATIONS of movies for the user
        int userId = this.usersHash.get(user);
        List<RecommendedItem> recommendations = recommender.recommend(userId, this.NUMBER_RECOMMENDATIONS);
        BiMap<Integer, String> productHashInverse = this.productHash.inverse();
        List<String> recommendationIds = recommendations.stream()
            .map(recommendation -> productHashInverse.get((int)recommendation.getItemID()))
            .collect(Collectors.toList());

        return recommendationIds;
    }

    /*
      first 10 lines of decompressed reviews:
        product/productId: B003AI2VGA
        review/userId: A141HP4LYPWMSR
        review/profileName: Brian E. Erland "Rainbow Sphinx"
        review/helpfulness: 7/7
        review/score: 3.0
        review/time: 1182729600
        review/summary: "There Is So Much Darkness Now ~ Come For The Miracle"
        review/text: Synopsis: On the daily trek from Juarez, Mexico to El Paso, Texas an ever increasing number
        of female workers are found raped and murdered in the surrounding desert. Investigative reporter Karina Danes (Minnie Driver) arrives from Los Angeles to pursue the story and angers both the local police and the factory owners who employee the undocumented aliens with her pointed questions and relentless quest for the truth.<br /><br />Her story goes nationwide when a young girl named Mariela (Ana Claudia Talancon) survives a vicious attack and walks out of the desert crediting the Blessed Virgin for her rescue. Her story is further enhanced when the "Wounds of Christ" (stigmata) appear in her palms. She also claims to have received a message of hope for the Virgin Mary and soon a fanatical movement forms around her to fight
        against the evil that holds such a stranglehold on the area.<br /><br />Critique: Possessing a lifelong fascination with such esoteric matters as Catholic mysticism, miracles and the mysterious appearance of the stigmata, I was immediately attracted to the '05 DVD release `Virgin of Juarez'. The film offers a rather unique storyline blending current socio-political concerns, the constant flow of Mexican migrant workers back and forth across the U.S./Mexican border and the traditional Catholic beliefs of the Hispanic population. I must say I was quite surprised by the unexpected route taken by the plot and the means and methods by which the heavenly message unfolds.<br /><br />`Virgin of Juarez' is not a film that you would care to watch over and over again, but it was interesting enough to merit at least one viewing. Minnie Driver delivers a solid performance and Ana Claudia Talancon is perfect as the fragile and innocent visionary Mariela. Also starring Esai Morales and Angus Macfadyen (Braveheart).

     */
    private void readWriteMovies(String moviesPath) throws IOException {
        File result = new File(this.OUTPUT_FILENAME);
        FileWriter writer = new FileWriter(result);
        int currentUser = 0;
        int currentProduct = 0;
        InputStream stream = new GZIPInputStream(new FileInputStream(moviesPath));
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

        String line = "";
        while((line = reader.readLine()) != null){
            if(line.startsWith("product/productId:")){

                String product = line.split(" ")[1];
                if(!this.productHash.containsKey(product)){
                    this.productHash.put(product, ++this.totalProducts);
                }
                currentProduct = this.productHash.get(product);

            }else if(line.startsWith("review/userId")){

                String userId = line.split(" ")[1];
                if(!this.usersHash.containsKey(userId)){
                    this.usersHash.put(userId, ++this.totalUsers);
                }
                currentUser = this.usersHash.get(userId);

            }else if(line.startsWith("review/score")){

                String score = line.split(" ")[1];
                writer.write(currentUser + "," + currentProduct + "," + score + "\n");
                this.totalReviews += 1;

            }
        }

        reader.close();
        writer.close();
    }

    // getters for the test
    public int getTotalReviews(){
        return this.totalReviews;
    }
    public int getTotalProducts(){
        return this.totalProducts;
    }
    public int getTotalUsers(){
        return this.totalUsers;
    }

}

