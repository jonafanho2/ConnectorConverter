import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Transforms input data of type I into output data of type O for connector pipelines.
 *
 * @param <I> The source/input data type
 * @param <O> The target/output data type
 */
public class ConnectorTransformer<I, O> {

    private final Function<I, O> mappingRule;

    public ConnectorTransformer(Function<I, O> mappingRule) {
        this.mappingRule = Objects.requireNonNull(mappingRule, "Mapping rule cannot be null");
    }

    /**
     * Transforms a single input record.
     */
    public O transform(I input) {
        if (input == null) {
            return null;
        }
        return mappingRule.apply(input);
    }

    /**
     * Transforms a collection of input records into a list of output records.
     */
    public List<O> transformAll(Collection<I> inputs) {
        if (inputs == null) {
            return List.of();
        }
        return inputs.stream()
                .map(this::transform)
                .collect(Collectors.toList());
    }

    /**
     * Chains another transformation step onto the output of this transformer.
     */
    public <V> ConnectorTransformer<I, V> andThen(Function<O, V> nextStep) {
        Objects.requireNonNull(nextStep, "Next step cannot be null");
        return new ConnectorTransformer<>(this.mappingRule.andThen(nextStep));
    }
}
