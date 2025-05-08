/*
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spendreport;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class FreezeDetector extends KeyedProcessFunction<Integer, FreezeDetectionJob.Sensor, Alert> {

    @Override
    public void processElement(
            FreezeDetectionJob.Sensor sensor,
            Context context,
            Collector<Alert> collector) throws Exception {

        if (sensor.getTemperature() < 0) {
            Alert alert = new Alert();
            alert.setId(sensor.getId());

            collector.collect(alert);
        }
    }
}
