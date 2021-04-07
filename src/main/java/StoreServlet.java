import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

@WebServlet(name = "StoreServlet", value = "/StoreServlet")
public class StoreServlet extends HttpServlet {

    private final static Jedis jedis = new Jedis("localhost");
    private final static int topK = 10;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
        res.setContentType("text/plain");
        String urlPath = req.getRequestURI();
        System.out.println(urlPath);
        // check we have a URL!
        if (urlPath == null || urlPath.isEmpty()) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            res.getWriter().write("missing parameters");
            return;
        }

        String[] urlParts = urlPath.split("/");
        // and now validate url path and return the response status code
        // (and maybe also some value if input is valid)
        System.out.println(Arrays.toString(urlParts));

        if (!isGetUrlValid(urlParts)) {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);
            res.getWriter().write("Invalid url for GET");
        } else {
            switch (urlParts[2]){
                // /items/store/{storeID}
                case "store":
                    // return the top 10 most purchased items at Store N
                    res.setStatus(HttpServletResponse.SC_OK);
                    // return the result if the id exist in memory
                    if(jedis.exists(urlParts[3])){
                        res.getWriter().write(top10ItemOfStore(urlParts[3]));
                    }else{
                        // return error message instead
                        res.getWriter().write("Invalid Store ID");
                    }
                    break;
                // /items/top10/{itemID}
                case "top10":
                    // return the top 10 stores for sales for item N
                    res.setStatus(HttpServletResponse.SC_OK);
                    // return the result if the id exist in memory
                    if(jedis.exists(urlParts[3])){
                        res.getWriter().write(top10StoreOfItem(urlParts[3]));
                    }else{
                        // return error message instead
                        res.getWriter().write("Invalid Item ID");
                    }
                    break;
                default:
                    res.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    res.getWriter().write("Invalid endpoint for GET");
                    break;
            }

        }
    }

    private boolean isGetUrlValid(String[] urlPath) {
        // /items/store/{storeID}
        // /items/top10/{itemID}
        if(urlPath.length != 4) {
            return false;
        } else if(!urlPath[1].equals("items")){
            return false;
        }
        return true;
    }

    private String top10ItemOfStore(String store_id){
        Map<String, String> items = jedis.hgetAll("store_" + store_id);

        Queue<String> minHeap = new PriorityQueue<>(
                Comparator.comparingInt(item -> Integer.parseInt(items.get(item)))
        );

        for (String item : items.keySet()){
            minHeap.add(item);
            if(minHeap.size() > topK) minHeap.poll();
        }

        ArrayDeque<String> stack = new ArrayDeque<>();
        for(int i = topK; i > 0; i--){
            if(!minHeap.isEmpty()){
                stack.push(minHeap.poll());
            }
        }
        StringBuilder sb = new StringBuilder();
        while(!stack.isEmpty()){
            String item = stack.pop();
            sb.append(item + ":" + items.get(item) + "\n");
        }

        return sb.toString();
    }

    private String top10StoreOfItem(String item_id){
        Map<String, String> stores = jedis.hgetAll(item_id);

        Queue<String> minHeap = new PriorityQueue<>(
                Comparator.comparingInt(item -> Integer.parseInt(stores.get(item)))
        );

        for (String store : stores.keySet()){
            minHeap.add(store);
            if(minHeap.size() > topK) minHeap.poll();
        }

        ArrayDeque<String> stack = new ArrayDeque<>();
        for(int i = topK; i > 0; i--){
            if(!minHeap.isEmpty()){
                stack.push(minHeap.poll());
            }
        }
        StringBuilder sb = new StringBuilder();
        while(!stack.isEmpty()){
            String store = stack.pop();
            sb.append(store + ":" + stores.get(store) + "\n");
        }

        return sb.toString();
    }

}
