package model.score;

import org.apache.hadoop.io.Writable;

public interface AbstractRiskScore extends Writable {

  double getRiskScore();
}
