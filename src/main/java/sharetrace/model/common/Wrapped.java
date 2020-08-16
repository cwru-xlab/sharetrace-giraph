package sharetrace.model.common;

import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;

@Value.Style(
    typeImmutable = "*",
    allParameters = true,
    visibility = ImplementationVisibility.PUBLIC,
    defaults = @Value.Immutable(builder = false, copy = false, prehash = true))
public @interface Wrapped {

}
