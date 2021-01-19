package org.sharetrace.model.pda.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.Optional;
import java.util.function.Predicate;

public final class PdaUtil {

  public static void verifyOptionalString(Optional<String> stringOptional,
      String exceptionMessage) {
    if (stringOptional.isPresent()) {
      String string = stringOptional.get();
      Preconditions.checkArgument(!Strings.isNullOrEmpty(string), exceptionMessage);
    }
  }

  public static <N extends Number> void verifyOptionalNumber(Optional<N> numberOptional,
      Predicate<N> condition, String exceptionMessage) {
    if (numberOptional.isPresent()) {
      N number = numberOptional.get();
      Preconditions.checkArgument(condition.test(number), exceptionMessage);
    }
  }
}
