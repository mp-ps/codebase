import vertexai
from flask import Flask
from flask_cors import CORS
from vertexai.language_models import (TextGenerationModel)

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

@app.route('/api/search', methods=['GET'])
def search_action():

    vertexai.init(
    project='burner-sidpatha',
    experiment='my-experiment',
    experiment_description='my experiment description'
    )
  
    parameters = {
        "temperature": 0.0,  # Temperature controls the degree of randomness in token selection.
        "max_output_tokens": 256,  # Token limit determines the maximum amount of text output.
        "top_p": 0.8,  # Tokens are selected from most probable to least until the sum of their probabilities equals the top_p value.
        "top_k": 40,  # A top_k of 1 means the selected token is the most probable among all tokens.
    }

    prompt_example="Write a 2-day itinerary for France."
    model = TextGenerationModel.from_pretrained("text-bison@001")
    response = model.predict(
        prompt=prompt_example, 
        **parameters
    )
    print(f"Response from Model: {response.text}")
    return response.candidates


if __name__ == "__main__":
    app.run(debug=True)
