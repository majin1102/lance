# HuggingFace Integration

The HuggingFace Hub has become the go to place for ML practitioners to find pre-trained models and useful datasets.

HuggingFace datasets can be written directly into Lance format by using the
`lance.write_dataset` method. You can write the entire dataset or a particular split. For example:

```python
import datasets # pip install datasets
import lance

lance.write_dataset(datasets.load_dataset(
    "poloclub/diffusiondb", split="train[:10]",
), "diffusiondb_train.lance")
``` 