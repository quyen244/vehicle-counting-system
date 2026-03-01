# ABSA Model Loading Fix

## Problem

The current model file `vietnamese_sentiment_model (1).keras` contains TextVectorization and StringLookup preprocessing layers with Vietnamese vocabulary. On Windows, these layers cannot be loaded due to encoding issues:

```
'charmap' codec can't decode byte 0x81 in position 188: character maps to <undefined>
```

## Solutions

### Option 1: Use Pre-trained Model Without Preprocessing Layers (RECOMMENDED)

Since the consumer already does text preprocessing in Python (`preprocess_text` function), the TextVectorization layer in the model is redundant.

**Create a model without preprocessing layers:**

```python
# In train_model.py or a separate script
import tensorflow as tf

# Load the existing model
full_model = tf.keras.models.load_model("vietnamese_sentiment_model (1).keras", compile=False)

# Get the model architecture without the preprocessing layers
# Assuming the model structure is: Input -> TextVectorization -> Embedding -> LSTM -> Dense -> Outputs
# We want to keep only: Embedding -> LSTM -> Dense -> Outputs

# Create a new model that skips the TextVectorization layer
# This requires knowing the model architecture

# For now, you can export just the weights and rebuild:
# OR use the model starting from after the TextVectorization layer
```

### Option 2: Export Model to ONNX Format

Convert the Keras model to ONNX format which handles encoding better:

```bash
pip install tf2onnx
python -m tf2onnx.convert --keras vietnamese_sentiment_model.keras --output model.onnx
```

### Option 3: Use SavedModel Format with UTF-8

```python
# When saving the model:
model.save('model_savedmodel', save_format='tf')  # Use SavedModel format

# When loading:
model = tf.saved_model.load('model_savedmodel')
```

### Option 4: Temporary Workaround - Mock Predictions

For testing purposes, you can temporarily use mock predictions to test the pipeline:

```python
def create_predict_udf(model_path):
    @pandas_udf(ArrayType(IntegerType()))
    def predict_udf(batch_series: pd.Series) -> pd.Series:
        # Return random predictions for testing
        import random
        return pd.Series([[random.choice([-1, 0, 1, 2]) for _ in range(8)] for _ in range(len(batch_series))])
    return predict_udf
```

## Recommended Actions

1. **Immediate Fix**: Use Option 4 (mock predictions) to test the pipeline
2. **Proper Solution**: Retrain or rebuild the model without TextVectorization layers since preprocessing is done in Python
3. **Alternative**: Convert model to ONNX or SavedModel format

## Files to Modify

If using mock predictions temporarily:
- `consumer.py`: Replace the predict_udf function

If rebuilding model:
- `train_model.py`: Remove TextVectorization layer from model architecture
- Train new model without preprocessing layers
- Update `AppConfig.MODEL_PATH` to point to new model
