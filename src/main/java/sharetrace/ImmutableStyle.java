package sharetrace;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;

@Target({ElementType.PACKAGE, ElementType.TYPE})
@Retention(RetentionPolicy.CLASS)
@Value.Style(
    get = {"is*", "get*"},
    init = "set*",
    typeImmutable = "*",
    allParameters = true,
    deepImmutablesDetection = true,
    depluralize = true,
    visibility = ImplementationVisibility.PUBLIC,
    defaults = @Value.Immutable(prehash = true)
)
public @interface ImmutableStyle {

}